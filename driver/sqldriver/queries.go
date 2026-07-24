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

func buildQueries(d Dialect, table string) queries {
	t := quoteIdent(d, table)
	k := quoteIdent(d, "key")
	v := quoteIdent(d, "value")
	e := quoteIdent(d, "expires_at")
	p := func(i int) string { return placeholder(d, i) }
	esc := `ESCAPE '\'`
	if d == MySQL {
		esc = `ESCAPE '\\'`
	}
	q := queries{
		table:         t,
		key:           k,
		get:           fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s AND (%s IS NULL OR %s > %s)", v, t, k, p(1), e, e, p(2)),
		del:           fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND (%s IS NULL OR %s > %s)", t, k, p(1), e, e, p(2)),
		delIfEquals:   fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND %s = %s AND (%s IS NULL OR %s > %s)", t, k, p(1), v, p(2), e, e, p(3)),
		deleteExpired: fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND %s IS NOT NULL AND %s <= %s", t, k, p(1), e, e, p(2)),
		clearPrefix:   fmt.Sprintf("DELETE FROM %s WHERE %s LIKE %s %s", t, k, p(1), esc),
		clearAll:      fmt.Sprintf("DELETE FROM %s", t),
	}
	switch d {
	case Postgres:
		q.upsert = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, $3) ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s", t, k, v, e, k, v, v, e, e)
		q.insertIgnore = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, $3) ON CONFLICT (%s) DO NOTHING", t, k, v, e, k)
		q.sweep = fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NOT NULL AND %s <= $1 LIMIT $2)", t, k, k, t, e, e)
	case MySQL:
		q.upsert = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE %s = ?, %s = ?", t, k, v, e, v, e)
		q.insertIgnore = fmt.Sprintf("INSERT IGNORE INTO %s (%s, %s, %s) VALUES (?, ?, ?)", t, k, v, e)
		q.sweep = fmt.Sprintf("DELETE FROM %s WHERE %s IS NOT NULL AND %s <= ? LIMIT ?", t, e, e)
	case SQLite:
		q.upsert = fmt.Sprintf("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?) ON CONFLICT(%s) DO UPDATE SET %s = excluded.%s, %s = excluded.%s", t, k, v, e, k, v, v, e, e)
		q.insertIgnore = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s, %s, %s) VALUES (?, ?, ?)", t, k, v, e)
		q.sweep = fmt.Sprintf("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s IS NOT NULL AND %s <= ? LIMIT ?)", t, k, k, t, e, e)
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
