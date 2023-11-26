package pg

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"runtime"
)

type MigrationState int

const (
	MigrationPending MigrationState = 0 // This migration has not been applied yet.
	MigrationSuccess                = 1 // This migration succeeded. (Applied)
	MigrationFailed                 = 2 // This migration failed. (Applied)
)

// MigrationStateInfo The state of a migration.
type MigrationStateInfo struct {
	DisplayName string
	Applied     bool
	Failed      bool
}

type MigrationInfo struct {
	Version       string         // The Version of the schema after the migration is complete.
	State         MigrationState // The state of the migration (MigrationPending, MigrationSuccess, ...)
	Description   string         // The description of the migration
	InstalledRank int            // The rank of this installed migration.
	Checksum      string         // Computed checksum of the migration.
}

func (i *MigrationInfo) Identifier() string {
	return fmt.Sprintf("version %s", i.Version)
}

type MigrationPrepare func(context *Migration)

type Migration struct {
	Repeat   bool
	Info     *MigrationInfo
	commands []migrationCommand
	Prepare  MigrationPrepare
}

// ExecSql Schedule the execution of an SQL command in this migration
func (m *Migration) ExecSql(sql string, args ...interface{}) {
	m.commands = append(m.commands, &migrationCommandSql{
		Sql:  sql,
		Args: args,
	})
	m.Info.Checksum = hash(m.Info.Checksum + hash(sql))
}

// ExecFn Schedule the execution of a golang command in this migration
func (m *Migration) ExecFn(name string, callback MigrationCommandFn, args ...interface{}) {
	_, fn, line, _ := runtime.Caller(1)
	m.commands = append(m.commands, &migrationCommandCallback{
		Caller:   fmt.Sprintf("%s:%d", fn, line),
		Callback: callback,
		Args:     args,
	})
	m.Info.Checksum = hash(m.Info.Checksum + hash(name))
}

type migrationCommand interface {
	run(db *Database, migration *Migration) error
	debug() string
}

type migrationCommandSql struct {
	Sql  string
	Args []interface{}
}

func (c *migrationCommandSql) run(db *Database, migration *Migration) error {
	_, err := db.Execute(c.Sql, c.Args...)
	return err
}

func (c *migrationCommandSql) debug() string {
	debugMsg := fmt.Sprintf("%s\n", c.Sql)
	for i, arg := range c.Args {
		debugMsg += fmt.Sprintf("    $%d = %v\n", i, arg)
	}
	return debugMsg
}

type MigrationCommandFn func(db *Database, migration *Migration, args ...interface{}) error

type migrationCommandCallback struct {
	Caller   string
	Callback MigrationCommandFn
	Args     []interface{}
}

func (c *migrationCommandCallback) run(db *Database, migration *Migration) error {
	return c.Callback(db, migration, c.Args...)
}

func (c *migrationCommandCallback) debug() string {
	debugMsg := fmt.Sprintf("function %v\n", c.Caller)
	for i, arg := range c.Args {
		debugMsg += fmt.Sprintf("    $%d = %v\n", i, arg)
	}
	return debugMsg
}

func hash(text string) string {
	h := md5.New()
	h.Write([]byte(text))
	return hex.EncodeToString(h.Sum(nil))
}
