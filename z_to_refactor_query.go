package pg

import (
	"database/sql"
	"strings"
)

type Query struct {
	query   string
	retries int
	mapper  func(rows *Row) (model any, err error)
	db      *Database
}

type Row struct {
	rows *sql.Rows
	row  *sql.Row
}

func (r *Row) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *Row) ColumnTypes() ([]*sql.ColumnType, error) {
	return r.rows.ColumnTypes()
}

func (r *Row) Scan(dest ...any) error {
	if r.row != nil {
		return r.row.Scan(dest...)
	}
	return r.rows.Scan(dest...)
}

func NewQuery(query string, mapper func(rows *Row) (model any, err error)) *Query {
	query = strings.Join(strings.Fields(strings.TrimSpace(query)), " ")
	return &Query{
		query:  query,
		mapper: mapper,
	}
}

func (q *Query) With(db *Database) *Query {
	return &Query{
		db:      db,
		retries: q.retries,
		query:   q.query,
		mapper:  q.mapper,
	}
}

func (q *Query) Retry(retries int) *Query {
	return &Query{
		retries: retries,
		db:      q.db,
		query:   q.query,
		mapper:  q.mapper,
	}
}

func (q *Query) SelectAll(args ...any) (result []any, err error) {
	var rows *sql.Rows
	var statement *sql.Stmt

	// https://github.com/lib/pq/issues/635
	// https://github.com/lib/pq/issues/81
	if statement, err = q.db.Prepare(q.query); err != nil {
		return nil, err
	}

	if rows, err = statement.Query(args...); err != nil {
		_ = statement.Close()
		return
	}
	defer func() {
		_ = rows.Close()
		_ = statement.Close()
	}()

	row := &Row{rows: rows}

	var m any
	for rows.Next() {
		if m, err = q.mapper(row); err != nil {
			return
		}
		result = append(result, m)
	}
	return
}

func (q *Query) SelectOne(args ...any) (result any, err error) {
	var row *sql.Row
	var statement *sql.Stmt

	// https://github.com/lib/pq/issues/635
	// https://github.com/lib/pq/issues/81
	if statement, err = q.db.Prepare(q.query); err != nil {
		return
	}
	defer statement.Close()

	if row = statement.QueryRow(args...); row == nil {
		return nil, nil
	}

	if result, err = q.mapper(&Row{row: row}); err == sql.ErrNoRows {
		err = nil
		result = nil
	}
	return
}
