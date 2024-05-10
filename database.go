package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type Database struct {
	db         *sql.DB
	tx         *sql.Tx
	conn       *sql.Conn
	logger     Logger
	config     *Config
	migrations []*Migration
	id         string
}

// Config database config
type Config struct {
	Username string              // The username to connect with.
	Password string              // The password to connect with
	Host     string              // Specifies the host name on which PostgreSQL is running.
	Port     int                 // The TCP port of the PostgreSQL server.
	Database string              // The PostgreSQL database to connect to.
	SSLMode  string              // Controls whether SSL is used, depending on server support.
	Params   map[string][]string // Connection params
	DebugSql bool                // debug queries
	Logger   Logger              // Logger instance
}

func (c *Config) ConnString(customParams map[string]string) string {
	if c.Params == nil {
		c.Params = map[string][]string{}
	}
	params := url.Values(c.Params)

	if c.SSLMode != "" {
		params.Set("sslmode", c.SSLMode)
	}

	if customParams != nil {
		for k, v := range customParams {
			params.Set(k, v)
		}
	}

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Database,
		RawQuery: params.Encode(),
		Fragment: "",
	}

	return u.String()
}

// Open opens a database
func Open(config *Config) (*Database, error) {
	connString := config.ConnString(nil)
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	if config.Logger == nil {
		config.Logger = defaultLogger()
	}

	instance := &Database{
		db:     db,
		logger: config.Logger,
		config: config,
	}

	instancesMu.Lock()
	id := hash(connString)
	for {
		if _, exist := instances[id]; exist {
			id = hash(id)
		} else {
			break
		}
	}
	instance.id = id
	if instances == nil {
		instances = make(map[string]*Database)
	}
	instances[id] = instance
	instancesMu.Unlock()

	return instance, nil
}

// Close closes the database and prevents new queries from starting.
func (d *Database) Close() error {
	if d.id != "" {
		instancesMu.Lock()
		delete(instances, d.id)
		d.id = ""
		instancesMu.Unlock()
	}

	if err := d.db.Close(); err != nil {
		return err
	}

	return nil
}

// Conn returns a Database with a new connection
//
// Every Conn must be returned to the pool after use by calling Database.CloseConn.
func (d *Database) Conn() (*Database, error) {
	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	return &Database{
		db:     d.db,
		conn:   conn,
		logger: d.logger,
		config: d.config,
	}, nil
}

// Begin starts a transaction.
func (d *Database) Begin() (*Database, error) {
	if d.tx != nil {
		return d, nil
	}

	return d.BeginTx(context.Background(), nil)
}

// BeginTx starts a transaction.
func (d *Database) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Database, error) {
	var tx *sql.Tx
	var err error
	if d.conn != nil {
		tx, err = d.conn.BeginTx(ctx, opts)
	} else {
		tx, err = d.db.BeginTx(ctx, opts)
	}

	if err != nil {
		return nil, err
	}

	return &Database{
		tx:     tx,
		db:     d.db,
		conn:   d.conn,
		logger: d.logger,
		config: d.config,
	}, nil
}

// Commit commits the transaction.
func (d *Database) Commit() error {
	if d.tx != nil {
		err := d.tx.Commit()
		if err == nil {
			d.tx = nil
		} else {
			return errors.New("Unable to commit transaction (cause: " + err.Error() + ")")
		}
	}

	return nil
}

// Rollback aborts the transaction.
func (d *Database) Rollback() error {
	if d.tx != nil {
		err := d.tx.Rollback()
		if err == nil {
			d.tx = nil
		} else {
			return errors.New("Unable to rollback transaction. (cause: " + err.Error() + ")")
		}
	}

	return nil
}

// CloseConn returns the connection to the connection pool.
func (d *Database) CloseConn() error {
	err := d.Rollback()
	if err != nil {
		return err
	}

	if d.conn != nil {
		err = d.conn.Close()
		if err != nil {
			return err
		}
		d.conn = nil
	}

	return nil
}

func QuoteLiteral(literal string) string {
	return pq.QuoteLiteral(literal)
}

func QuoteIdentifier(name string) string {
	return pq.QuoteIdentifier(name)
}
