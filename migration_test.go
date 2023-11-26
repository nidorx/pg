package pg

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/dhui/dktest"
)

const (
	pgPassword = "postgres"
)

var (
	opts = dktest.Options{
		Env:          map[string]string{"POSTGRES_PASSWORD": pgPassword},
		PortRequired: true,
		ReadyFunc:    isReady,
	}
	// Supported versions: https://www.postgresql.org/support/versioning/
	specs = []ContainerSpec{
		{ImageName: "postgres:9.5", Options: opts},
		//{ImageName: "postgres:9.6", Options: opts},
		//{ImageName: "postgres:10", Options: opts},
		//{ImageName: "postgres:11", Options: opts},
		//{ImageName: "postgres:12", Options: opts},
		//{ImageName: "postgres:13", Options: opts},
		//{ImageName: "postgres:14", Options: opts},
		//{ImageName: "postgres:15", Options: opts},
		//{ImageName: "postgres:16", Options: opts},
	}
)

// ContainerSpec holds Docker testing setup specifications
type ContainerSpec struct {
	ImageName string
	Options   dktest.Options
}

// parallelTest runs Docker tests in parallel
func parallelTest(t *testing.T, specs []ContainerSpec, testFunc func(*testing.T, dktest.ContainerInfo)) {

	for i, spec := range specs {
		spec := spec // capture range variable, see https://goo.gl/60w3p2

		// Only test against one version in short mode
		// TODO: order is random, maybe always pick first version instead?
		if i > 0 && testing.Short() {
			t.Logf("Skipping %v in short mode", spec.ImageName)
		} else {
			t.Run(spec.ImageName, func(t *testing.T) {
				t.Parallel()
				dktest.Run(t, spec.ImageName, spec.Options, testFunc)
			})
		}
	}
}

func pgConnectionString(host, port string, options ...string) string {
	options = append(options, "sslmode=disable")
	return fmt.Sprintf("postgres://postgres:%s@%s:%s/postgres?%s", pgPassword, host, port, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	db, err := sql.Open("postgres", pgConnectionString(ip, port))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch {
		case errors.Is(err, sqldriver.ErrBadConn), err == io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

//go:embed testing/migrations
var migrationsFs embed.FS

func Test(t *testing.T) {
	parallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		portInt, _ := strconv.Atoi(port)
		db, err := Open(&Config{
			Username: "postgres",
			Password: "postgres",
			Host:     ip,
			Port:     portInt,
			Database: "postgres",
			SSLMode:  "disable",
			DebugSql: true,
			Logger:   nil,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.CloseConn(); err != nil {
				t.Error(err)
			}
		}()

		err = db.AddMigrations(migrationsFs)
		if err != nil {
			t.Fatal(err)
		}

		err = db.Migrate(nil)
		if err != nil {
			t.Fatal(err)
		}

		// dt.Test(t, db, []byte("SELECT 1"))
	})
}
