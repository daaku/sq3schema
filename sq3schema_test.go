package sq3schema_test

import (
	"context"
	"testing"

	"github.com/daaku/ensure"
	"github.com/daaku/sq3schema"
)

func TestMigrate(t *testing.T) {
	migrations := []string{
		"create table hello(world text)",
		"create unique index hello_index on hello (world)",
		"insert into hello values('42')",
	}
	db := sq3schema.MemDB(migrations)
	conn := db.Get(context.Background())
	defer db.Put(conn)
	stmt := conn.Prep("select * from hello")
	defer stmt.Finalize()
	_, err := stmt.Step()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, stmt.ColumnText(0), "42")
	ensure.Nil(t, sq3schema.Migrate(conn, migrations))
}
