package sqldriver

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/drivertest"
	_ "modernc.org/sqlite"
)

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newTestDriver(t *testing.T, opts ...Option) *Driver {
	t.Helper()
	d, err := New(newDB(t), SQLite, append([]Option{WithSweepInterval(0)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestConformance(t *testing.T) {
	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			t.Helper()
			return newTestDriver(t)
		},
	})
}

func TestNewValidatesInput(t *testing.T) {
	if _, err := New(nil, SQLite); err == nil {
		t.Fatal("expected error for nil db")
	}
	db := newDB(t)
	if _, err := New(db, "oracle"); err == nil {
		t.Fatal("expected error for unknown dialect")
	}
	if _, err := New(db, SQLite, WithTable("gocache; DROP TABLE users")); err == nil {
		t.Fatal("expected error for invalid table name")
	}
	if _, err := New(db, SQLite, WithSweepTimeout(0)); err == nil {
		t.Fatal("expected error for zero sweep timeout")
	}
	if _, err := New(db, SQLite, WithSweepTimeout(-time.Second)); err == nil {
		t.Fatal("expected error for negative sweep timeout")
	}
	if _, err := New(db, SQLite, WithSweepBatchSize(0)); err == nil {
		t.Fatal("expected error for zero sweep batch size")
	}
	if _, err := New(db, SQLite, WithSweepInterval(-time.Second)); err == nil {
		t.Fatal("expected error for negative sweep interval")
	}
	d, err := New(db, SQLite, WithSweepInterval(0))
	if err != nil {
		t.Fatalf("zero sweep interval must disable the sweeper, got %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClearPrefixEscapesLikeWildcards(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	if err := d.Set(ctx, "a%b:1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "axb:1", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.ClearPrefix(ctx, "a%b:"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Get(ctx, "a%b:1"); ok {
		t.Fatal("literal prefix not cleared")
	}
	if _, ok, _ := d.Get(ctx, "axb:1"); !ok {
		t.Fatal("wildcard leaked into the LIKE pattern")
	}
}

func TestSweepOnceDeletesExpiredRowsInBatches(t *testing.T) {
	d := newTestDriver(t, WithSweepBatchSize(2))
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		if err := d.Set(ctx, k, []byte("v"), 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Set(ctx, "live", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	n, err := d.SweepOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("swept %d rows", n)
	}
	var count int
	if err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM "gocache"`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows left = %d", count)
	}
}

func TestCloseStopsSweeperAndKeepsDBOpen(t *testing.T) {
	db := newDB(t)
	d, err := New(db, SQLite, WithSweepInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("driver closed the caller's db: %v", err)
	}
}

func TestDialectSQL(t *testing.T) {
	pg := buildQueries(Postgres, "gocache")
	if !strings.Contains(pg.get, "$1") {
		t.Fatalf("postgres get: %s", pg.get)
	}
	if !strings.Contains(pg.insertIgnore, "ON CONFLICT") || !strings.Contains(pg.insertIgnore, "DO NOTHING") {
		t.Fatalf("postgres add: %s", pg.insertIgnore)
	}
	if !strings.Contains(pg.sweep, "IN (SELECT") || !strings.Contains(pg.sweep, "LIMIT $1") {
		t.Fatalf("postgres sweep: %s", pg.sweep)
	}
	if !strings.Contains(pg.clearPrefix, `ESCAPE '\'`) {
		t.Fatalf("postgres clear prefix: %s", pg.clearPrefix)
	}

	my := buildQueries(MySQL, "gocache")
	if strings.Contains(my.get, "$1") {
		t.Fatalf("mysql must use ? placeholders: %s", my.get)
	}
	if !strings.Contains(my.upsert, "ON DUPLICATE KEY UPDATE") || strings.Contains(my.upsert, "VALUES(") {
		t.Fatalf("mysql upsert must not use the deprecated VALUES() form: %s", my.upsert)
	}
	if !strings.Contains(my.insertIgnore, "INSERT IGNORE") {
		t.Fatalf("mysql add: %s", my.insertIgnore)
	}
	if strings.Contains(my.sweep, "IN (SELECT") || !strings.Contains(my.sweep, "LIMIT ?") {
		t.Fatalf("mysql cannot LIMIT inside an IN subquery: %s", my.sweep)
	}
	if !strings.Contains(my.clearPrefix, `ESCAPE '\\'`) {
		t.Fatalf("mysql clear prefix: %s", my.clearPrefix)
	}
	if !strings.Contains(my.table, "`") {
		t.Fatalf("mysql identifiers must be backquoted: %s", my.table)
	}

	lite := buildQueries(SQLite, "gocache")
	if !strings.Contains(lite.insertIgnore, "INSERT OR IGNORE") {
		t.Fatalf("sqlite add: %s", lite.insertIgnore)
	}
	if !strings.Contains(lite.upsert, "excluded.") {
		t.Fatalf("sqlite upsert: %s", lite.upsert)
	}
}

func countPlaceholders(d Dialect, stmt string) int {
	if d == Postgres {
		return strings.Count(stmt, "$")
	}
	return strings.Count(stmt, "?")
}

func TestDialectSQLJudgesExpiryByDatabaseServerTime(t *testing.T) {
	for _, tc := range []struct {
		dialect Dialect
		now     string
		upsert  int
	}{
		{Postgres, "(EXTRACT(EPOCH FROM now()) * 1000)::bigint", 3},
		{MySQL, "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)", 5},
		{SQLite, "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)", 3},
	} {
		t.Run(string(tc.dialect), func(t *testing.T) {
			q := buildQueries(tc.dialect, "gocache")
			stmts := []struct {
				name string
				sql  string
				args int
			}{
				{"get", q.get, 1},
				{"upsert", q.upsert, tc.upsert},
				{"insertIgnore", q.insertIgnore, 3},
				{"del", q.del, 1},
				{"delIfEquals", q.delIfEquals, 2},
				{"deleteExpired", q.deleteExpired, 1},
				{"sweep", q.sweep, 1},
			}
			for _, s := range stmts {
				if !strings.Contains(s.sql, tc.now) {
					t.Errorf("%s does not read the clock from the database: %s", s.name, s.sql)
				}
				if n := countPlaceholders(tc.dialect, s.sql); n != s.args {
					t.Errorf("%s takes %d parameters, want %d — a client clock value is still bound: %s", s.name, n, s.args, s.sql)
				}
			}
			for _, s := range []struct {
				name string
				sql  string
			}{{"upsert", q.upsert}, {"insertIgnore", q.insertIgnore}} {
				if !strings.Contains(s.sql, tc.now+" + ") {
					t.Errorf("%s must offset the server clock by a ttl parameter: %s", s.name, s.sql)
				}
			}
		})
	}
}

func TestExpiryIsWrittenAndJudgedByTheDatabase(t *testing.T) {
	d := newTestDriver(t)
	ctx := context.Background()
	if err := d.Set(ctx, "gone", []byte("v"), 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "kept", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Set(ctx, "long", []byte("v"), 5*time.Minute); err != nil {
		t.Fatal(err)
	}

	expiry := func(key string) sql.NullInt64 {
		t.Helper()
		var v sql.NullInt64
		if err := d.db.QueryRowContext(ctx, `SELECT "expires_at" FROM "gocache" WHERE "key" = ?`, key).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if e := expiry("kept"); e.Valid {
		t.Fatalf("a zero ttl must store a NULL expiry, got %d", e.Int64)
	}
	var dbNow int64
	if err := d.db.QueryRowContext(ctx, `SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`).Scan(&dbNow); err != nil {
		t.Fatal(err)
	}
	e := expiry("long")
	if !e.Valid {
		t.Fatal("a positive ttl must store an expiry")
	}
	if delta := e.Int64 - dbNow; delta <= 0 || delta > (5*time.Minute).Milliseconds() {
		t.Fatalf("expiry is not database now + ttl: %d ms away from the server clock", delta)
	}

	time.Sleep(150 * time.Millisecond)
	if _, ok, _ := d.Get(ctx, "gone"); ok {
		t.Fatal("a row written with a ttl did not expire")
	}
	if _, ok, _ := d.Get(ctx, "kept"); !ok {
		t.Fatal("a row written without a ttl expired")
	}
	if ok, err := d.Add(ctx, "gone", []byte("v2"), time.Hour); err != nil || !ok {
		t.Fatalf("add did not reclaim an expired row: ok=%v err=%v", ok, err)
	}
	if ok, err := d.DeleteIfEquals(ctx, "kept", []byte("v")); err != nil || !ok {
		t.Fatalf("delete if equals on a live row: ok=%v err=%v", ok, err)
	}
	if ok, err := d.Delete(ctx, "long"); err != nil || !ok {
		t.Fatalf("delete on a live row: ok=%v err=%v", ok, err)
	}
}

func TestWithLoggerNilNormalizesToDiscardHandler(t *testing.T) {
	db := newDB(t)
	d, err := New(db, SQLite, WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if d.logger == nil {
		t.Fatal("logger should be normalized to non-nil discard handler")
	}
}

func TestSweeperNoCrashWithNilLogger(t *testing.T) {
	db := newDB(t)
	d, err := New(db, SQLite, WithLogger(nil), WithSweepInterval(5*time.Millisecond), WithSweepTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
