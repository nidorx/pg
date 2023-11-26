package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
)

var ErrOptimisticLock = errors.New("optimistic locking conflict occurs")

// RowWraper Wraper para trabalhar com o sql.Row, que tem propriedades privadas
type RowWraper struct {
	// One of these two will be non-nil:
	err  error
	row  *sql.Row
	rows *sql.Rows
}

func (r *RowWraper) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}

	return r.row.Scan(dest...)
}

func (r *RowWraper) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.row.Err()
}

// SelectRowWhere Executa um SELECT FROM WHERE
func (d *Database) SelectRowWhere(table string, fields map[string]interface{}, condition map[string]interface{}) error {

	var dest []any
	query := "SELECT "
	for key, ref := range fields {
		query += QuoteIdentifier(key) + ", "
		dest = append(dest, ref)
	}
	query = query[:len(query)-2] + " FROM " + QuoteIdentifier(table) + " WHERE "

	var i = 1
	var args []any
	for key, value := range condition {
		query += QuoteIdentifier(key) + " = $" + (strconv.Itoa(i)) + " AND "
		args = append(args, value)
		i++
	}
	query = query[:len(query)-5]

	return d.QueryRowOld(query, args...).Scan(dest...)
}

// InsertInto Executa um Insert Into
func (d *Database) InsertInto(schema, table string, values map[string]interface{}) (sql.Result, error) {

	var i = 1
	var args []any

	query := "INSERT INTO " + QuoteIdentifier(schema) + "." + QuoteIdentifier(table) + " ("
	sqlValues := ") VALUES ("
	for key, value := range values {
		query += QuoteIdentifier(key) + ", "
		sqlValues += "$" + (strconv.Itoa(i)) + ", "
		args = append(args, value)
		i++
	}
	query = query[:len(query)-2] + sqlValues[:len(sqlValues)-2] + ")"

	return d.Execute(query, args...)
}

// DeleteWhere Executa um DELETE FROM WHERE
func (d *Database) DeleteWhere(table string, condition map[string]interface{}) (sql.Result, error) {

	var i = 1
	var args = []interface{}{}

	query := "DELETE FROM " + QuoteIdentifier(table) + " WHERE "
	for key, value := range condition {
		query += QuoteIdentifier(key) + " = $" + (strconv.Itoa(i)) + " AND "
		args = append(args, value)
		i++
	}
	query = query[:len(query)-5]

	return d.Execute(query, args...)
}

// Update Executa uma query UPDATE SET values WHERE condition
func (d *Database) Update(
	schema, table string, values map[string]interface{}, condition map[string]interface{},
) (sql.Result, error) {

	var i = 1
	var args []any

	query := "UPDATE " + QuoteIdentifier(schema) + "." + QuoteIdentifier(table) + " SET "
	for key, value := range values {
		query += QuoteIdentifier(key) + " = $" + (strconv.Itoa(i)) + ", "
		args = append(args, value)
		i++
	}
	query = query[:len(query)-2] + " WHERE "

	for key, value := range condition {
		query += QuoteIdentifier(key) + " = $" + (strconv.Itoa(i)) + " AND "
		args = append(args, value)
		i++
	}
	query = query[:len(query)-5] // remove " AND "

	return d.Execute(query, args...)
}

// UpdateOptimisticLock Executa uma query UPDATE SET values WHERE condition
func (d *Database) UpdateOptimisticLock(
	schema, table string, values map[string]interface{}, condition map[string]interface{},
) (sql.Result, error) {
	result, err := d.Update(schema, table, values, condition)
	if err != nil {
		return nil, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		// optimistic locking exception
		return nil, ErrOptimisticLock
	}
	return result, err
}

// Upsert Executa uma query INSERT INTO ON CONFLICT UPDATE SET
func (d *Database) Upsert(table string, values map[string]interface{}, conflictField string) (sql.Result, error) {

	// @TODO: ordenar keys para que o prepare statement nao seja comprometido

	var i = 1
	var args = []interface{}{}

	sql := "INSERT INTO " + QuoteIdentifier(table) + " ("
	sqlValues := ") VALUES ("
	sqlUpdate := ") ON CONFLICT (" + QuoteIdentifier(conflictField) + ") DO UPDATE SET "
	for key, value := range values {
		sql += QuoteIdentifier(key) + ", "
		sqlValues += "$" + (strconv.Itoa(i)) + ", "
		args = append(args, value)
		if key != conflictField {
			sqlUpdate += QuoteIdentifier(key) + " = $" + (strconv.Itoa(i)) + ", "
		}
		i++
	}
	sql = sql[:len(sql)-2] + sqlValues[:len(sqlValues)-2] + sqlUpdate[:len(sqlUpdate)-2]

	return d.Execute(sql, args...)
}

// Query executes a prepared query statement with the given arguments
// and returns the query results as a *Rows.
func (d *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	d.debugQuery(query, args...)

	statement, err := d.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer statement.Close()

	return statement.Query(args...)
}

func (d *Database) QueryRow(query string, args ...interface{}) (row *sql.Row, err error) {
	d.debugQuery(query, args...)

	var statement *sql.Stmt

	if statement, err = d.Prepare(query); err != nil {
		return
	}

	defer statement.Close()

	return statement.QueryRow(args...), nil
}

func (d *Database) QueryRowOld(query string, args ...interface{}) *RowWraper {
	d.debugQuery(query, args...)

	statement, err := d.Prepare(query)
	if err != nil {
		return &RowWraper{err: err}
	}

	defer statement.Close()

	return &RowWraper{row: statement.QueryRow(args...)}
}

func (d *Database) QueryForBoolean(query string, args ...interface{}) (bool, error) {

	d.debugQuery(query, args...)

	statement, err := d.Prepare(query)

	if err != nil {
		return false, err
	}

	defer statement.Close()

	var result bool
	err = statement.QueryRow(args...).Scan(&result)
	return result, err
}

// QueryForInt Obtém o resultado de uma query que busca por um valor. IMPORTANTE! Quando a query nao retornar registros
// esse método ira retornar 0 como resposta
func (d *Database) QueryForInt(query string, args ...interface{}) (int64, error) {

	d.debugQuery(query, args...)

	statement, err := d.Prepare(query)

	if err != nil {
		return 0, err
	}

	defer statement.Close()

	var result int64
	err = statement.QueryRow(args...).Scan(&result)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return result, err
}

func (d *Database) Prepare(query string) (*sql.Stmt, error) {
	var statement *sql.Stmt
	var err error

	if d.tx != nil {
		statement, err = d.tx.Prepare(query)
	} else if d.conn != nil {
		statement, err = d.conn.PrepareContext(context.Background(), query)
	} else {
		statement, err = d.db.Prepare(query)
	}
	return statement, err
}

// Execute executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (d *Database) Execute(query string, args ...interface{}) (sql.Result, error) {
	d.debugQuery(query, args...)

	if d.tx != nil {
		return d.tx.Exec(query, args...)
	} else if d.conn != nil {
		return d.conn.ExecContext(context.Background(), query, args...)
	} else {
		return d.db.Exec(query, args...)
	}
}

// Savepoint define a new savepoint within the current transaction
func (d *Database) Savepoint(savepoint string, callback func() error) error {
	db, err := d.Begin()
	if err != nil {
		return err
	}

	_, err = db.Execute("SAVEPOINT " + savepoint)
	if err != nil {
		return err
	}

	ch := make(chan bool)

	go func() {
		// panic control to avoid connection deadlock
		defer func() {
			if p := recover(); p != nil {
				err = errors.New(fmt.Sprintf("%v\n%s", p, string(debug.Stack())))
			}
			close(ch)
		}()

		// executes this callback within a transaction
		err = callback()
	}()

	<-ch

	if err != nil {
		_, errR := db.Execute("ROLLBACK TO SAVEPOINT " + savepoint)
		if errR != nil {
			err = errors.Join(errR, err)
		}
		return err
	}

	return nil
}

// Transaction Executes this callback within a transaction
func (d *Database) Transaction(callback func(db *Database) error) error {

	db, err := d.Begin()
	if err != nil {
		return err
	}

	ch := make(chan bool)

	go func() {
		// panic control to avoid connection deadlock
		defer func() {
			if p := recover(); p != nil {
				err = errors.New(fmt.Sprintf("%v\n%s", p, string(debug.Stack())))
			}
			close(ch)
		}()

		// executes this callback within a transaction
		err = callback(db)
	}()

	<-ch

	if err == nil {
		err = db.Commit()
	} else {
		err = errors.Join(db.Rollback(), err)
	}

	return err
}

func (d *Database) debugQuery(query string, args ...interface{}) {
	if !d.config.DebugSql {
		return
	}
	msg := "\n    " + strings.ReplaceAll(query, "\n", "\n    ")
	if len(args) > 0 {
		msg += "\n"
		for i, arg := range args {
			msg += fmt.Sprintf("    - $%d = %v\n", i+1, arg)
		}
		msg = msg[:len(msg)-1]
	}
	d.logger.Info(msg)
}
