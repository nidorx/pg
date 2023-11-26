package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nidorx/retry"
	"golang.org/x/mod/semver"
)

type migrationHistory struct {
	db                 *Database
	dbLock             *Database
	dbSchema           *Database
	cache              []*MigrationInfo
	tableName          string
	schemaName         string
	lastAppliedVersion string
	logger             Logger
}

func (h *migrationHistory) Migrate() error {

	h.lastAppliedVersion = "0"

	migrations := h.db.migrations

	sort.SliceStable(migrations, func(i, j int) bool {
		a := migrations[i]
		b := migrations[j]
		if a.Repeat == b.Repeat {
			return semver.Compare("v"+a.Info.Version, "v"+b.Info.Version) < 0
		}
		if a.Repeat {
			return false
		}
		return true
	})

	// init context (fast fail)
	for _, migration := range migrations {
		migration.Prepare(migration)
	}

	if err := h.createTable(); err != nil {
		return err
	}

	totalSuccess := 0
	start := time.Now()

	for {

		count := 0

		// acquire the lock now. The lock will be released at the end of each migration.
		err := h.lock(func() error {
			var err error
			count, err = h.migrateNext(totalSuccess == 0, migrations)
			return err
		})

		if err != nil {
			return err
		}

		totalSuccess += count
		if count == 0 {
			// no further migrations available
			break
		}
	}

	h.log(totalSuccess, time.Since(start).Milliseconds(), h.lastAppliedVersion)
	return nil
}

func (h *migrationHistory) migrateNext(firstRun bool, migrations []*Migration) (int, error) {

	appliedMigrations, err := h.getAppliedMigrations()
	if err != nil {
		return 0, err
	}

	lastAppliedVersion := ""
	notResolved := map[string]*MigrationInfo{}
	appliedByVersion := map[string]*MigrationInfo{}

	for _, info := range appliedMigrations {
		version := info.Version
		if version != "R" {
			notResolved[version] = info
			appliedByVersion[version] = info
			if info.State == MigrationSuccess && semver.Compare("v"+version, "v"+lastAppliedVersion) > 0 {
				lastAppliedVersion = version
			}
		}
	}

	h.lastAppliedVersion = lastAppliedVersion

	if firstRun {
		h.logger.Info("Current version of schema %s: %s", h.schemaName, lastAppliedVersion)
	}

	var pendingMigrations []*Migration

	// compare with local migrations
	for _, migration := range migrations {
		resolved := migration.Info
		version := resolved.Version

		notResolved[version] = nil

		if version != "R" {
			resolved.State = MigrationPending
		}

		applied := appliedByVersion[version]
		if applied == nil {
			// has not yet been applied
			if version != "R" && semver.Compare("v"+version, "v"+lastAppliedVersion) <= 0 {
				msg := fmt.Sprintf(
					"Schema %s has a version (%s) that is newer than the available migration (%s).",
					h.schemaName, lastAppliedVersion, version,
				)
				return 0, errors.New(msg)
			}
		} else if applied.State == MigrationSuccess {
			// If it has already been successfully applied to the base, check if there have been any local changes
			if applied.Checksum != resolved.Checksum {

				debugMsg := "\n------------------------------------------------------------------------------\n"
				debugMsg += fmt.Sprintf("Migration - %s - %s", resolved.Identifier(), resolved.Description)
				debugMsg += "\n------------------------------------------------------------------------------\n"
				for i, cmd := range migration.commands {
					debugMsg += fmt.Sprintf("-- (%d)\n", i+1)
					debugMsg += cmd.debug()
					debugMsg += "\n"
				}
				debugMsg = debugMsg[:len(debugMsg)-1]
				debugMsg += "------------------------------------------------------------------------------\n"
				h.logger.Info(debugMsg)

				return 0, errors.New(mismatchMessage("checksum", resolved.Identifier(), applied.Checksum, resolved.Checksum))
			}

			// verifica descrição
			if applied.Description != resolved.Description {
				return 0, errors.New(mismatchMessage("description", resolved.Identifier(), applied.Description, resolved.Description))
			}

			// marca a versao local como aplicada com sucesso
			resolved.State = MigrationSuccess
		}

		if resolved.State == MigrationPending {
			pendingMigrations = append(pendingMigrations, migration)
		}
	}

	// Verifica migrations que foram removidas do código (nunca pode acontecer)
	for _, info := range notResolved {
		if info != nil {
			return 0, errors.New("Detected applied migration not resolved locally: " + info.Identifier() + "")
		}
	}

	// nao existe migration pendente
	if len(pendingMigrations) == 0 {
		return 0, nil
	}

	// Obtém a próxima migration que sera executada
	migration := pendingMigrations[0]

	start := time.Now()

	// finally applies the migration. The migration state and time are updated accordingly.
	err = h.migrateSingle(migration)
	if err != nil {
		h.logger.Warn(
			"Migration of %s failed!\n    Caused by: %s\n    Changes successfully rolled back.",
			toMigrationText(migration), err.Error(),
		)
		executionTime := time.Since(start)
		err2 := h.addAppliedMigration(migration.Info, int(executionTime.Milliseconds()), false)
		if err2 != nil {
			h.logger.Error(err2)
		}
		return 0, err
	}

	h.lastAppliedVersion = migration.Info.Version

	return 1, nil
}

func (h *migrationHistory) migrateSingle(migration *Migration) error {

	start := time.Now()
	migrationText := toMigrationText(migration)

	h.logger.Info("Starting migration of %s ...", migrationText)

	newDbSchemaConn, err := h.dbSchema.Conn()
	if err != nil {
		return err
	}

	defer func() {
		if errRelease := newDbSchemaConn.CloseConn(); errRelease != nil {
			h.logger.Error(errRelease)
		}
	}()

	err = newDbSchemaConn.Transaction(func(db *Database) error {
		for _, cmd := range migration.commands {
			if errExec := cmd.run(db, migration); errExec != nil {
				return errors.New(fmt.Sprintf("Migration failed !\n    Caused by: %s", errExec.Error()))
			}
		}
		h.logger.Info("Successfully completed migration of " + migrationText)
		return nil
	})
	if err != nil {
		return err
	}

	executionTime := time.Since(start)

	// atualiza informações sobre a migration local
	migration.Info.State = MigrationSuccess

	return h.addAppliedMigration(migration.Info, int(executionTime.Milliseconds()), true)
}

func (h *migrationHistory) createTable() error {

	if exists, err := h.schemaExists(); err != nil {
		return err
	} else if !exists {
		if errCreateSchema := h.createSchema(); errCreateSchema != nil {
			return errCreateSchema
		}
	}

	if dbSchema, err := h.newSchemaConnection(h.schemaName); err != nil {
		return err
	} else {
		h.dbSchema = dbSchema
	}

	if tableExists, err := h.tableExists(); err != nil {
		return err
	} else if tableExists {
		return nil
	}

	table := h.tableName
	sqlCreateTable := strings.Join([]string{
		"CREATE TABLE " + table + " (",
		"   installed_rank INT NOT NULL PRIMARY KEY,",
		"   version VARCHAR(50),",
		"   description VARCHAR(200) NOT NULL,",
		"   checksum CHARACTER(32),",
		"   installed_on TIMESTAMP NOT NULL DEFAULT now(),",
		"   execution_time INTEGER NOT NULL,",
		"   success BOOLEAN NOT NULL",
		")"}, "\n")

	sqlCreateIndex := "CREATE INDEX " + QuoteIdentifier(table+"_s_idx") + " ON " + QuoteIdentifier(table) + " (success)"

	retries := retry.New(10, func(ctx context.Context, err error, attempt int, willRetry bool, nextRetry time.Duration) {
		h.db.logger.Warn("Schema migrationHistory table creation failed. cause: %v", err)
		if willRetry {
			h.db.logger.Info("Retrying in %s", (nextRetry).String())
		}
	})

	err := retries.Execute(context.Background(), func(ctx context.Context, attempt int) error {
		if tableExists, err := h.tableExists(); err != nil {
			return err
		} else if tableExists {
			return nil
		}

		if attempt == 1 {
			h.db.logger.Info("Creating Schema migrationHistory table " + table + " ...")
		}

		err := h.dbSchema.Transaction(func(db *Database) error {
			_, err := db.Execute(sqlCreateTable)
			if err != nil {
				return err
			}

			_, err = db.Execute(sqlCreateIndex)
			return err
		})
		if err == nil {
			h.db.logger.Info("Created Schema migrationHistory table " + table)
		}

		return err
	})

	return err
}

func (h *migrationHistory) newSchemaConnection(schema string) (*Database, error) {
	d := h.db
	connStr := d.config.ConnString(map[string]string{"search_path": schema})
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(fmt.Sprintf("Unable to connect to database: %v", err))
	}

	return &Database{
		db:     db,
		logger: d.logger,
		config: d.config,
	}, nil
}

func (h *migrationHistory) schemaExists() (bool, error) {
	exist, err := h.db.QueryForBoolean(
		"SELECT EXISTS (SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1)",
		h.schemaName,
	)
	if err != nil {
		return false, errors.New(fmt.Sprintf("unable to check whether schema " + h.schemaName + " exists (cause: " + err.Error() + ")"))
	}
	return exist, nil
}

func (h *migrationHistory) tableExists() (bool, error) {
	exist, err := h.db.QueryForBoolean(strings.Join([]string{
		"SELECT EXISTS (",
		"    SELECT 1 FROM pg_catalog.pg_class c",
		"    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace",
		"    WHERE n.nspname = $1",
		"    AND c.relname = $2",
		"    AND c.relkind = 'r'",
		")",
	}, "\n"), h.schemaName, h.tableName)
	if err != nil {
		return false, errors.New(fmt.Sprintf("unable to check whether table " + h.tableName + " exists (cause: " + err.Error() + ")"))
	}

	return exist, nil
}

func (h *migrationHistory) createSchema() error {

	retries := retry.New(10, func(ctx context.Context, err error, attempt int, willRetry bool, nextRetry time.Duration) {
		h.db.logger.Warn("Schema %s creation failed.", h.schemaName)
		if willRetry {
			h.db.logger.Info("Retrying in %s", (nextRetry).String())
		}
	})

	err := retries.Execute(context.Background(), func(ctx context.Context, attempt int) error {
		if exists, err := h.schemaExists(); err != nil {
			return err
		} else if exists {
			return nil
		}

		if attempt == 1 {
			h.db.logger.Info("Creating Schema " + h.schemaName + " ...")
		}

		err := h.db.Transaction(func(db *Database) error {
			_, err := db.Execute("CREATE SCHEMA " + h.schemaName + ";")
			return err
		})
		if err == nil {
			h.db.logger.Info("Created Schema " + h.schemaName)
		}

		return err
	})

	return err
}

// addAppliedMigration Records a new applied migration.
func (h *migrationHistory) addAppliedMigration(info *MigrationInfo, executionTime int, success bool) error {
	if h.dbLock == nil {
		return errors.New("method can only be invoked when table is locked")
	}

	// removes any previous faults
	table := h.tableName
	_, err := h.dbLock.Execute("DELETE FROM "+table+" WHERE version = $1", info.Version)
	if err != nil {
		return errors.New(fmt.Sprintf(
			"Unable to delete failed row for version %s in Schema migrationHistory table %s (cause: %s)",
			info.Version, table, err.Error(),
		))
	}

	installedRank, err := h.calculateInstalledRank()
	if err == nil {
		_, err = h.dbLock.InsertInto(h.schemaName, table, map[string]interface{}{
			"installed_rank": installedRank,
			"version":        info.Version,
			"description":    info.Description,
			"checksum":       info.Checksum,
			"installed_on":   time.Now().UTC().Format(time.RFC3339),
			"execution_time": executionTime,
			"success":        success,
		})
	}

	if err != nil {
		return errors.New(fmt.Sprintf(
			"Unable to insert row for version %s in Schema migrationHistory table %s (cause: %s)",
			info.Version, table, err.Error(),
		))
	}

	info.InstalledRank = installedRank

	return nil
}

// calculateInstalledRank  Calculates the installed rank for the new migration to be inserted.
// This is the most precise way to sort applied migrations by installation order.
// Migrations that were applied later have a higher rank. (Only for applied migrations)
func (h *migrationHistory) calculateInstalledRank() (int, error) {
	appliedMigrations, err := h.getAppliedMigrations()
	if err != nil {
		return 0, err
	}

	if len(appliedMigrations) == 0 {
		return 1, nil
	}

	return appliedMigrations[len(appliedMigrations)-1].InstalledRank + 1, nil
}

// lock Acquires an exclusive read-write lock on the schema history table. This lock will be released automatically upon completion.
func (h *migrationHistory) lock(callback func() error) error {

	if h.dbLock != nil {
		// It is not allowed to invoke this method twice, it only expects one lock at a time
		return errors.New("schema migrationHistory table is already locked")
	}

	// get exclusive connection
	lockDb, err := h.dbSchema.Conn()
	if err != nil {
		return errors.New("Unable to lock Schema migrationHistory table (cause: " + err.Error() + ")")
	}

	// release connection
	defer func() {
		h.dbLock = nil
		if errRelease := lockDb.CloseConn(); errRelease != nil {
			h.db.logger.Error(errRelease)
		}
	}()

	h.dbLock = lockDb

	var cbErr error
	err = lockDb.Transaction(func(db *Database) error {
		// lock table
		// https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-TABLES
		_, err = db.Execute("SELECT * FROM " + h.tableName + " FOR UPDATE")
		if err != nil {
			return errors.New("Unable to lock Schema migrationHistory table (cause: " + err.Error() + ")")
		}

		cbErr = callback()

		return nil
	})

	if cbErr != nil {
		return cbErr
	}

	return err
}

// getAppliedMigrations The list of all migrations applied on the schemaName in the order they were applied (oldest first).
// An empty list if no migration has been applied so far.
func (h *migrationHistory) getAppliedMigrations() ([]*MigrationInfo, error) {

	maxCachedInstalledRank := -1

	cacheLen := len(h.cache)
	if cacheLen > 0 {
		h.sortCache()
		maxCachedInstalledRank = h.cache[cacheLen-1].InstalledRank
	}

	table := h.tableName

	// See https://www.pgpool.net/docs/latest/en/html/runtime-config-load-balancing.html
	query := strings.Join([]string{
		"/*NO LOAD BALANCE*/",
		"SELECT installed_rank, version, description, checksum, success",
		"FROM " + table,
		"WHERE  installed_rank > $1",
		"ORDER BY  installed_rank",
	}, " ")

	rows, err := h.dbSchema.Query(query, maxCachedInstalledRank)
	if err != nil {
		return nil, errors.New(fmt.Sprintf(
			"Error while retrieving the list of applied migrations from Schema migrationHistory table "+table+" (cause %s)", err.Error(),
		))
	}
	defer rows.Close()

	for rows.Next() {
		var u MigrationInfo
		var success bool
		if err := rows.Scan(&u.InstalledRank, &u.Version, &u.Description, &u.Checksum, &success); err != nil {
			return nil, errors.New(fmt.Sprintf(
				"Error while retrieving the list of applied migrations from Schema migrationHistory table "+table+" (cause %s)", err.Error(),
			))
		}

		if success {
			u.State = MigrationSuccess
		} else {
			u.State = MigrationFailed
		}

		h.cache = append(h.cache, &u)
	}

	h.sortCache()
	return h.cache, nil
}

func (h *migrationHistory) sortCache() {
	sort.SliceStable(h.cache, func(i, j int) bool {
		return h.cache[i].InstalledRank < h.cache[j].InstalledRank
	})
}

func (h *migrationHistory) log(successCount int, executionTime int64, schemaVersion string) {
	if successCount == 0 {
		h.logger.Info("Schema is up to date. No migration necessary.")
		return
	}

	migrationText := "migration"
	if successCount > 1 {
		migrationText = "migrations"
	}

	h.logger.Info(
		"Successfully applied %d %s to schema, now at version v%s (execution time %dms)",
		successCount, migrationText, schemaVersion, executionTime,
	)
}

func toMigrationText(migration *Migration) string {
	return fmt.Sprintf("schema to version %s (%s)", migration.Info.Version, migration.Info.Description)
}

func mismatchMessage(mismatch string, migrationIdentifier string, applied string, resolved string) string {
	return fmt.Sprintf("Migration "+mismatch+" mismatch for migration %s\n"+
		"-> Applied to database : %s\n"+
		"-> Resolved locally    : %s"+
		". Revert the changes to the migration, or update the schema history.", migrationIdentifier, applied, resolved)
}
