// Package sqldriver provides a cache driver backed by a SQL database.
//
// It suits deployments that already run a database and would rather not add
// Redis for caching. It is slower than a dedicated cache under load, and puts
// write traffic on a database usually sized for something else, so prefer Redis
// for a hot L2.
//
// Postgres, MySQL and SQLite are supported. Expiry is enforced in SQL — every
// read filters on the expiry column — so an entry past its TTL is never
// returned even before it is deleted. A background sweeper removes expired rows
// in batches so the table does not grow without bound.
//
// The table must exist before use; see [Driver.Migrate] to create it, or
// [Driver.Schema] to fold the statements into your own migrations.
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

// Dialect selects the SQL a driver generates. Placeholder syntax, upsert form
// and current-time expression differ enough between engines that the driver
// cannot be dialect-agnostic.
type Dialect string

// The supported SQL dialects. Passing anything else to [New] is an error.
const (
	Postgres Dialect = "postgres"
	MySQL    Dialect = "mysql"
	SQLite   Dialect = "sqlite"
)

const deleteBatch = 500

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Option configures a [Driver].
type Option func(*Driver)

// WithTable sets the table name, defaulting to "gocache". The name is
// validated as a plain SQL identifier and rejected otherwise, since it is
// interpolated into statements rather than passed as a parameter.
func WithTable(name string) Option {
	return func(d *Driver) { d.table = name }
}

// WithSweepInterval sets how often expired rows are deleted, defaulting to 5
// minutes. Zero disables the sweeper, leaving expired rows in place until
// something else removes them; reads still filter them out. It must not be
// negative.
func WithSweepInterval(v time.Duration) Option {
	return func(d *Driver) { d.sweepInterval = v }
}

// WithSweepBatchSize caps how many rows one sweep deletes, defaulting to 1000.
// Sweeping in batches keeps each delete's lock short on a large table. It must
// be positive.
func WithSweepBatchSize(n int) Option {
	return func(d *Driver) { d.sweepBatch = n }
}

// WithSweepTimeout bounds a single sweep, defaulting to 30 seconds, so a
// blocked delete cannot stall the sweeper indefinitely. It must be positive.
func WithSweepTimeout(v time.Duration) Option {
	return func(d *Driver) { d.sweepTimeout = v }
}

// WithLogger sets where background sweep failures are reported, defaulting to
// slog.Default(). Sweep errors are logged rather than returned, since no caller
// is waiting on them.
func WithLogger(l *slog.Logger) Option {
	return func(d *Driver) { d.logger = l }
}

// Driver stores cache entries in a SQL table. Use [New] to create one. It is
// safe for concurrent use, and owns a background sweeper that [Driver.Close]
// stops.
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

// New returns a driver backed by db. The database handle must not be nil and
// the dialect must be one of [Postgres], [MySQL] or [SQLite].
//
// New does not create the table; call [Driver.Migrate] or apply [Driver.Schema]
// yourself. It starts the background sweeper, so the driver must be closed.
//
// The driver does not own db: [Driver.Close] stops the sweeper but leaves the
// pool open for the rest of the application.
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
	if err := d.validate(); err != nil {
		return nil, err
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

func (d *Driver) validate() error {
	switch d.dialect {
	case Postgres, MySQL, SQLite:
	default:
		return fmt.Errorf("sqldriver: unknown dialect %q", d.dialect)
	}
	if !identPattern.MatchString(d.table) {
		return fmt.Errorf("sqldriver: invalid table name %q", d.table)
	}
	if d.sweepInterval < 0 {
		return errors.New("sqldriver: sweep interval must not be negative")
	}
	if d.sweepBatch < 1 {
		return errors.New("sqldriver: sweep batch size must be positive")
	}
	if d.sweepTimeout < 1 {
		return errors.New("sqldriver: sweep timeout must be positive")
	}
	return nil
}

// Schema returns the statements that create the cache table and its indexes
// for the configured dialect, so they can be folded into an existing migration
// tool instead of running Driver.Migrate.
func (d *Driver) Schema() []string {
	return slices.Clone(d.q.schema)
}

// Migrate creates the cache table and its indexes if they do not exist. It is
// safe to call on every startup.
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

// Get implements gocache.Reader. Expiry is applied in SQL, so a row past its
// TTL is a miss even if the sweeper has not deleted it yet.
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

// Set implements gocache.Writer as an upsert. A ttl of zero or less stores
// the entry with no expiry.
func (d *Driver) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if _, err := d.db.ExecContext(ctx, d.q.upsert, d.upsertArgs(key, value, ttlMS(ttl))...); err != nil {
		return fmt.Errorf("sqldriver: set: %w", err)
	}
	return nil
}

// Add implements gocache.Atomic. It runs in a transaction that first clears
// an expired row, so a conflict is decided by the database rather than by a
// read-then-write race in the driver.
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

// Delete implements gocache.Writer, reporting whether a live row was
// removed. Deleting an expired row reports false.
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

// DeleteMany implements gocache.Writer. Keys are deleted in batches, so a
// failure partway through may leave some already removed.
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

// DeleteIfEquals implements gocache.Atomic. The comparison is part of the
// DELETE's WHERE clause, so it is atomic without a transaction.
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

// ClearPrefix implements gocache.Writer with a LIKE match. Metacharacters in
// the prefix are escaped so they cannot widen the match. An empty prefix
// deletes every row in the table.
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

// SweepOnce deletes one batch of expired rows and reports how many it removed,
// bounded by [WithSweepBatchSize]. The background sweeper calls it on a timer;
// call it directly to drive cleanup from your own scheduler, or in tests where
// waiting for the timer is impractical.
//
// A return equal to the batch size means more expired rows probably remain.
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

// Close implements io.Closer. It stops the background sweeper and waits for
// an in-flight sweep to finish, leaving the database handle open.
func (d *Driver) Close() error {
	d.once.Do(func() {
		d.cancel()
		d.wg.Wait()
	})
	return nil
}
