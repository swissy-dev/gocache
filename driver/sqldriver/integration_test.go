//go:build integration

package sqldriver

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/drivertest"
)

func runIntegration(t *testing.T, envVar, sqlDriver string, dialect Dialect) {
	t.Helper()
	dsn := os.Getenv(envVar)
	if dsn == "" {
		t.Skipf("%s is not set", envVar)
	}
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	newDriver := func(t *testing.T, opts ...Option) *Driver {
		t.Helper()
		base := []Option{WithTable("gocache_it"), WithSweepInterval(0)}
		d, err := New(db, dialect, append(base, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.Close() })
		return d
	}

	setup := newDriver(t)
	if err := setup.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = setup.ClearPrefix(context.Background(), "") })

	drivertest.Run(t, drivertest.Config{
		New: func(t *testing.T) gocache.Driver {
			d := newDriver(t)
			if err := d.ClearPrefix(context.Background(), ""); err != nil {
				t.Fatal(err)
			}
			return d
		},
	})

	t.Run("Sweep", func(t *testing.T) {
		d := newDriver(t, WithSweepBatchSize(2))
		ctx := context.Background()
		if err := d.ClearPrefix(ctx, ""); err != nil {
			t.Fatal(err)
		}
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
		if _, ok, _ := d.Get(ctx, "live"); !ok {
			t.Fatal("sweeper removed a live row")
		}
	})
}

func TestPostgresConformance(t *testing.T) {
	runIntegration(t, "GOCACHE_TEST_POSTGRES", "pgx", Postgres)
}

func TestMySQLConformance(t *testing.T) {
	runIntegration(t, "GOCACHE_TEST_MYSQL", "mysql", MySQL)
}
