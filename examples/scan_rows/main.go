package main

import (
	"context"
	"log"

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
	cols = append(cols, cmock.ColumnType{Type: "String", Name: "content"})

	values := make([][]any, 1)
	values[0] = make([]any, 3)
	values[0][0] = int32(42069)
	values[0][1] = "Inside the Making of CoComelon, the Children's Entertainment Juggernaut"
	values[0][2] = "..."

	rows := cmock.NewRows(cols, values)

	mock.
		ExpectQuery("SELECT id, title, content FROM articles WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT id, title, content FROM articles WHERE id = ?", 1)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when querying a statement", err)
	}

	cnt := 0
	for returnRows.Next() {
		var id int32
		var title string
		var content string
		err = returnRows.Scan(&id, &title, &content)
		if err != nil {
			log.Fatalf("an error '%s' was not expected when scanning a row", err)
		}

		if id != 42069 {
			log.Fatalf("expected id to be 42069, but got %d", id)
		}

		if title != "Inside the Making of CoComelon, the Children's Entertainment Juggernaut" {
			log.Fatalf("expected title to be `Inside the Making of CoComelon, the Children's Entertainment Juggernaut`, but got `%s`", title)
		}

		if content != "..." {
			log.Fatalf("expected content to be content, but got %s", content)
		}
		cnt++

		if cnt > 2 {
			log.Fatalf("expected only 1 row, but got more")
			break
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		log.Fatalf("there were unfulfilled expectations: %s", err)
	}
}
