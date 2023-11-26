package pg

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"golang.org/x/mod/semver"
)

// MigrationConfig database config
type MigrationConfig struct {
	Username string // The username to connect with.
	Password string // The password to connect with.
	Schema   string // migrationHistory schema name (defaults public)
	Table    string // migrationHistory table name (defaults pg_schema_history)
}

// Migrate run all migrations
func (d *Database) Migrate(config *MigrationConfig) error {

	if d.migrations != nil {
		if config == nil {
			config = &MigrationConfig{}
		}

		if config.Username == "" {
			config.Username = d.config.Username
		}

		if config.Password == "" {
			config.Password = d.config.Password
		}

		if config.Schema == "" {
			config.Schema = "public"
		}

		if config.Table == "" {
			config.Table = "pg_schema_history"
		}

		db := d
		if config.Username != d.config.Username {
			var err error
			db, err = Open(&Config{
				Username: config.Username,
				Password: config.Password,
				Host:     d.config.Host,
				Port:     d.config.Port,
				Database: d.config.Database,
				SSLMode:  d.config.SSLMode,
				Params:   d.config.Params,
				DebugSql: d.config.DebugSql,
				Logger:   d.config.Logger,
			})
			if err != nil {
				return err
			}
			db.migrations = d.migrations
			defer db.Close()
		}

		history := &migrationHistory{
			db:         db,
			logger:     d.logger,
			schemaName: config.Schema,
			tableName:  config.Table,
		}

		if err := history.Migrate(); err != nil {
			return err
		}

		d.migrations = nil
		db.migrations = nil
	}

	return nil
}

// AddMigrations automatically registers all migration files in a directory.
func (d *Database) AddMigrations(dir fs.FS) error {
	err := fs.WalkDir(dir, ".", func(filepath string, entry fs.DirEntry, err error) error {
		if entry.IsDir() || !strings.HasSuffix(filepath, ".sql") {
			return nil
		}

		content, err := fs.ReadFile(dir, filepath)
		if err != nil {
			return err
		}

		parts := strings.Split(strings.TrimSpace(strings.TrimSuffix(path.Base(filepath), ".sql")), "_")
		if len(parts) < 2 {
			// v1.0.0_Analytics_Schema.sql
			return errors.New("invalid migration name:" + filepath)
		}
		version := strings.TrimPrefix(parts[0], "v")
		description := strings.Join(parts[1:], " ")

		return d.AddMigration(version, description, func(migration *Migration) {
			migration.ExecSql(string(content))
		})
	})

	return err
}

// AddMigration register a new migration
func (d *Database) AddMigration(version, description string, prepare MigrationPrepare) error {

	if version != "R" {
		if valid := semver.IsValid("v" + version); !valid {
			return errors.New(fmt.Sprintf("migration has a invalid semantic version (%s)", version))
		}
	}

	if dl := len(description); dl == 0 {
		return errors.New(fmt.Sprintf("migration description is required (v%s)", version))
	} else if dl > 200 {
		description = string([]rune(description)[:200])
	}

	migration := &Migration{
		Prepare: prepare,
		Repeat:  version == "R",
		Info: &MigrationInfo{
			Version:       version,
			State:         MigrationPending,
			Description:   description,
			InstalledRank: 0,
			Checksum:      "",
		},
	}

	// check for duplicated version
	for _, m := range d.migrations {
		if m.Info.Version == version {
			return errors.New(fmt.Sprintf(
				"found more than one migration with version %s\nOffenders:\n-> %s\n-> %s",
				version, m.Info.Description, migration.Info.Description,
			))
		}
	}

	d.migrations = append(d.migrations, migration)

	return nil
}
