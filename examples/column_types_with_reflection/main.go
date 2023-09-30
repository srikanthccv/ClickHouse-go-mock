package main

import (
	"context"
	"log"
	"reflect"

	cmock "github.com/srikanthccv/ClickHouse-go-mock"
)

func main() {
	mock, err := cmock.NewClickHouseNative(nil)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := make([]cmock.ColumnType, 0)
	cols = append(cols, cmock.ColumnType{Type: "Int32", Name: "id"})
	cols = append(cols, cmock.ColumnType{Type: "String", Name: "title"})

	values := make([][]any, 1)
	values[0] = make([]any, 2)
	values[0][0] = int32(1)
	values[0][1] = "title"

	rows := cmock.NewRows(cols, values)

	mock.
		ExpectQuery("SELECT * FROM articles WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 1)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when querying a statement", err)
	}

	var (
		columnTypes = rows.ColumnTypes()
		vars        = make([]any, len(columnTypes))
	)
	for i := range columnTypes {
		vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
	}
	for returnRows.Next() {
		var id int32
		var title string
		if err := rows.Scan(vars...); err != nil {
			log.Fatalf("an error '%s' was not expected when scanning a row", err)
		}
		for _, v := range vars {
			switch v := v.(type) {
			case *int32:
				id = *v
			case *string:
				title = *v
			}
		}

		if id != 1 {
			log.Fatalf("expected id to be 1, but got %d", id)
		}

		if title != "title" {
			log.Fatalf("expected title to be title, but got %s", title)
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		log.Fatalf("there were unfulfilled expectations: %s", err)
	}
}
