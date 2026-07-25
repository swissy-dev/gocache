package sqldriver

import (
	"fmt"
	"strconv"
)

type queries struct {
	table         string
	key           string
	get           string
	upsert        string
	insertIgnore  string
	deleteExpired string
	del           string
	delIfEquals   string
	clearPrefix   string
	clearAll      string
	sweep         string
	schema        []string
}

func quoteIdent(d Dialect, name string) string {
	if d == MySQL {
		return "`" + name + "`"
	}
	return `"` + name + `"`
}

func placeholder(d Dialect, i int) string {
	if d == Postgres {
		return "$" + strconv.Itoa(i)
	}
	return "?"
}

func serverNow(d Dialect) string {
	switch d {
	case Postgres:
		return "(EXTRACT(EPOCH FROM now()) * 1000)::bigint"
	case MySQL:
		return "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)"
	case SQLite:
		return "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)"
	}
	return ""
}

func buildQueries(d Dialect, table string) queries {
	t := quoteIdent(d, table)
	k := quoteIdent(d, "key")
	v := quoteIdent(d, "value")
	e := quoteIdent(d, "expires_at")
	p := func(i int) string { return placeholder(d, i) }
	now := serverNow(d)
	expiry := func(i int) string { return now + " + " + p(i) }
	esc := `ESCAPE '\'`
	if d == MySQL {
		esc = `ESCAPE '\\'`
	}
	q := queries{
		table:         t,
		key:           k,
		get:           fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s AND (%s IS NULL OR %s > %s)", v, t, k, p(1), e, e, now),
		del:           fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND (%s IS NULL OR %s > %s)", t, k, p(1), e, e, now),
		delIfEquals:   fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND %s = %s AND (%s IS NULL OR %s > %s)", t, k, p(1), v, p(2), e, e, now),
		deleteExpired: fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND %s IS NOT NULL AND %s <= %s", t, k, p(1), e, e, now),
		clearPrefix:   fmt.Sprintf("DELETE FROM %s WHERE %s LIKE %s %s", t, k, p(1), esc),
		clearAll:      fmt.Sprintf("DELETE FROM %s", t),
	}
	switch d {
	case Postgres:
		q.upsert = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, %s) ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s", t, k, v, e, expiry(3), k, v, v, e, e)
		q.insertIgnore = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, %s) ON CONFLICT (%s) DO NOTHING", t, k, v, e, expiry(3), k)
		q.sweep = fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NOT NULL AND %s <= %s LIMIT $1)", t, k, k, t, e, e, now)
	case MySQL:
		q.upsert = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, %s) ON DUPLICATE KEY UPDATE %s = ?, %s = %s", t, k, v, e, expiry(3), v, e, expiry(5))
		q.insertIgnore = fmt.Sprintf("INSERT IGNORE INTO %s (%s, %s, %s) VALUES (?, ?, %s)", t, k, v, e, expiry(3))
		q.sweep = fmt.Sprintf("DELETE FROM %s WHERE %s IS NOT NULL AND %s <= %s LIMIT ?", t, e, e, now)
	case SQLite:
		q.upsert = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, %s) ON CONFLICT(%s) DO UPDATE SET %s = excluded.%s, %s = excluded.%s", t, k, v, e, expiry(3), k, v, v, e, e)
		q.insertIgnore = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s, %s, %s) VALUES (?, ?, %s)", t, k, v, e, expiry(3))
		q.sweep = fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NOT NULL AND %s <= %s LIMIT ?)", t, k, k, t, e, e, now)
	}
	q.schema = buildSchema(d, table, t, k, v, e)
	return q
}

func buildSchema(d Dialect, rawTable, t, k, v, e string) []string {
	idx := quoteIdent(d, rawTable+"_expires_at_idx")
	switch d {
	case Postgres:
		return []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s TEXT PRIMARY KEY, %s BYTEA NOT NULL, %s BIGINT)", t, k, v, e),
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", idx, t, e),
		}
	case MySQL:
		return []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s VARBINARY(255) NOT NULL PRIMARY KEY, %s BLOB NOT NULL, %s BIGINT NULL, INDEX %s (%s)) ENGINE=InnoDB", t, k, v, e, idx, e),
		}
	case SQLite:
		return []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s TEXT PRIMARY KEY, %s BLOB NOT NULL, %s INTEGER)", t, k, v, e),
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", idx, t, e),
		}
	}
	return nil
}
