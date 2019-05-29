// Package sq3schema exposes a simple API to utilize the user_version
// pragma to apply a slice of migrations to a sqlite database. This
// provides a simple schema migration facility.
//
// Remember the migrations slice should be considered "immutable",
// that is don't remove entries from in there. Just add entries, even if
// you want to drop tables.
package sq3schema // import "github.com/daaku/sq3schema"

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
	"golang.org/x/xerrors"
)

func getUserVersion(conn *sqlite.Conn) (int, error) {
	stmt := conn.Prep("PRAGMA user_version;")
	defer stmt.Finalize()
	_, err := stmt.Step()
	if err != nil {
		return 0, err
	}
	return stmt.ColumnInt(0), nil
}

// Migrate runs the migrations on the connection.
func Migrate(conn *sqlite.Conn, migrations []string) error {
	max := len(migrations) + 2
	for {
		max--
		if max == 0 {
			return xerrors.New("sq3schema: too many iterations")
		}
		currentVersion, err := getUserVersion(conn)
		if err != nil {
			return err
		}
		if currentVersion > len(migrations) {
			return xerrors.Errorf("sq3schema: unexpected higher version than known versions: %d",
				currentVersion)
		}
		if currentVersion == len(migrations) {
			return nil
		}
		err = sqlitex.ExecScript(conn, migrations[currentVersion]+
			fmt.Sprintf("; PRAGMA user_version = %d;", currentVersion+1))
		if err != nil {
			return xerrors.Errorf("sq3schema: error updating user_version: %w", err)
		}
	}
}

var memDB uint64

// MemDB returns a new in-memory DB Pool with the migrations already applied.
// This is meant to be used for tests, and so it panics on errors.
func MemDB(migrations []string) *sqlitex.Pool {
	c := atomic.AddUint64(&memDB, 1)
	uri := fmt.Sprintf("file:sb-mem-%d:?mode=memory", c)
	dbPool, err := sqlitex.Open(uri, 0, 1)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := dbPool.Get(ctx)
	defer dbPool.Put(conn)
	if err := Migrate(conn, migrations); err != nil {
		panic(err)
	}
	return dbPool
}
