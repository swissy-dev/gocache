package sqldriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

type Dialect string

const (
	Postgres Dialect = "postgres"
	MySQL    Dialect = "mysql"
	SQLite   Dialect = "sqlite"
)

const deleteBatch = 500

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Option func(*Driver)

func WithTable(name string) Option {
	return func(d *Driver) { d.table = name }
}

func WithSweepInterval(v time.Duration) Option {
	return func(d *Driver) { d.sweepInterval = v }
}

func WithSweepBatchSize(n int) Option {
	return func(d *Driver) { d.sweepBatch = n }
}

func WithSweepTimeout(v time.Duration) Option {
	return func(d *Driver) { d.sweepTimeout = v }
}

func WithLogger(l *slog.Logger) Option {
	return func(d *Driver) { d.logger = l }
}

type Driver struct {
	db            *sql.DB
	dialect       Dialect
	table         string
	sweepInterval time.Duration
	sweepBatch    int
	sweepTimeout  time.Duration
	logger        *slog.Logger
	q             queries
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	once          sync.Once
}

func New(db *sql.DB, dialect Dialect, opts ...Option) (*Driver, error) {
	if db == nil {
		return nil, errors.New("sqldriver: nil db")
	}
	d := &Driver{
		db:            db,
		dialect:       dialect,
		table:         "gocache",
		sweepInterval: 5 * time.Minute,
		sweepBatch:    1000,
		sweepTimeout:  30 * time.Second,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		opt(d)
	}
	switch d.dialect {
	case Postgres, MySQL, SQLite:
	default:
		return nil, fmt.Errorf("sqldriver: unknown dialect %q", d.dialect)
	}
	if !identPattern.MatchString(d.table) {
		return nil, fmt.Errorf("sqldriver: invalid table name %q", d.table)
	}
	if d.sweepInterval < 0 {
		return nil, errors.New("sqldriver: sweep interval must not be negative")
	}
	if d.sweepBatch < 1 {
		return nil, errors.New("sqldriver: sweep batch size must be positive")
	}
	if d.sweepTimeout < 1 {
		return nil, errors.New("sqldriver: sweep timeout must be positive")
	}
	if d.logger == nil {
		d.logger = slog.New(slog.DiscardHandler)
	}
	d.q = buildQueries(d.dialect, d.table)
	d.ctx, d.cancel = context.WithCancel(context.Background())
	if d.sweepInterval > 0 {
		d.wg.Add(1)
		go d.sweepLoop()
	}
	return d, nil
}

func (d *Driver) Schema() []string {
	return slices.Clone(d.q.schema)
}

func (d *Driver) Migrate(ctx context.Context) error {
	for _, stmt := range d.q.schema {
		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqldriver: migrate: %w", err)
		}
	}
	return nil
}

func ttlMS(ttl time.Duration) any {
	if ttl <= 0 {
		return nil
	}
	return max(ttl.Milliseconds(), 1)
}

func (d *Driver) upsertArgs(key string, value []byte, ttl any) []any {
	args := []any{key, value, ttl}
	if d.dialect == MySQL {
		args = append(args, value, ttl)
	}
	return args
}

func (d *Driver) Get(ctx context.Context, key string) ([]byte, bool, error) {
	var value []byte
	err := d.db.QueryRowContext(ctx, d.q.get, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqldriver: get: %w", err)
	}
	return value, true, nil
}

func (d *Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if _, err := d.db.ExecContext(ctx, d.q.upsert, d.upsertArgs(key, value, ttlMS(ttl))...); err != nil {
		return fmt.Errorf("sqldriver: set: %w", err)
	}
	return nil
}

func (d *Driver) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("sqldriver: add begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, d.q.deleteExpired, key); err != nil {
		return false, fmt.Errorf("sqldriver: add cleanup: %w", err)
	}
	res, err := tx.ExecContext(ctx, d.q.insertIgnore, key, value, ttlMS(ttl))
	if err != nil {
		return false, fmt.Errorf("sqldriver: add insert: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqldriver: add rows affected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("sqldriver: add commit: %w", err)
	}
	return n > 0, nil
}

func (d *Driver) Delete(ctx context.Context, key string) (bool, error) {
	res, err := d.db.ExecContext(ctx, d.q.del, key)
	if err != nil {
		return false, fmt.Errorf("sqldriver: delete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqldriver: delete rows affected: %w", err)
	}
	return n > 0, nil
}

func (d *Driver) deleteManyQuery(n int) string {
	ph := make([]string, n)
	for i := range ph {
		ph[i] = placeholder(d.dialect, i+1)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", d.q.table, d.q.key, strings.Join(ph, ", "))
}

func (d *Driver) DeleteMany(ctx context.Context, keys []string) error {
	for chunk := range slices.Chunk(keys, deleteBatch) {
		args := make([]any, len(chunk))
		for i, k := range chunk {
			args[i] = k
		}
		if _, err := d.db.ExecContext(ctx, d.deleteManyQuery(len(chunk)), args...); err != nil {
			return fmt.Errorf("sqldriver: delete many: %w", err)
		}
	}
	return nil
}

func (d *Driver) DeleteIfEquals(ctx context.Context, key string, value []byte) (bool, error) {
	res, err := d.db.ExecContext(ctx, d.q.delIfEquals, key, value)
	if err != nil {
		return false, fmt.Errorf("sqldriver: delete if equals: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqldriver: delete if equals rows affected: %w", err)
	}
	return n > 0, nil
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (d *Driver) ClearPrefix(ctx context.Context, prefix string) error {
	if prefix == "" {
		if _, err := d.db.ExecContext(ctx, d.q.clearAll); err != nil {
			return fmt.Errorf("sqldriver: clear: %w", err)
		}
		return nil
	}
	if _, err := d.db.ExecContext(ctx, d.q.clearPrefix, escapeLike(prefix)+"%"); err != nil {
		return fmt.Errorf("sqldriver: clear prefix: %w", err)
	}
	return nil
}

func (d *Driver) SweepOnce(ctx context.Context) (int64, error) {
	var total int64
	for {
		sctx, cancel := context.WithTimeout(ctx, d.sweepTimeout)
		res, err := d.db.ExecContext(sctx, d.q.sweep, d.sweepBatch)
		cancel()
		if err != nil {
			return total, fmt.Errorf("sqldriver: sweep: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("sqldriver: sweep rows affected: %w", err)
		}
		total += n
		if n < int64(d.sweepBatch) {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

func (d *Driver) sweepLoop() {
	defer d.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			d.logger.Error("sqldriver: sweeper panic", "panic", p)
		}
	}()
	ticker := time.NewTicker(d.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			if _, err := d.SweepOnce(d.ctx); err != nil && d.ctx.Err() == nil {
				d.logger.Warn("sqldriver: sweep failed", "err", err)
			}
		}
	}
}

func (d *Driver) Close() error {
	d.once.Do(func() {
		d.cancel()
		d.wg.Wait()
	})
	return nil
}
