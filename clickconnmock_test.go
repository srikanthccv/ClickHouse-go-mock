// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockhouse

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestPrepareExpectations(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	append := mock.
		ExpectPrepareBatch("INSERT INTO articles (id, title, content) VALUES (?, ?, ?)").
		ExpectAppend()
	if append == nil {
		t.Errorf("stmt was expected while creating a prepared statement")
	}

	var clickConn = mock
	batch, err := clickConn.PrepareBatch(context.Background(), "INSERT INTO articles (id, title, content) VALUES (?, ?, ?)")
	if err != nil {
		t.Errorf("an error '%s' was not expected when preparing a batch statement", err)
	}

	batch.Append(1, "title", "content")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryExpectations(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT id, title, content FROM articles WHERE id = ?").WithArgs(1)
	_, err = mock.Query(context.Background(), "SELECT id, title, content FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryExepectationsWithArgs(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT id, title, content FROM articles WHERE id = ?").WithArgs(1)
	_, err = mock.Query(context.Background(), "SELECT id, title, content FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryExepectationsWithArgsAndRows(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := make([]ColumnType, 0)
	cols = append(cols, ColumnType{Type: "Int32", Name: "id"})
	cols = append(cols, ColumnType{Type: "String", Name: "title"})
	cols = append(cols, ColumnType{Type: "String", Name: "content"})

	values := make([][]any, 1)
	values[0] = make([]any, 3)
	values[0][0] = int32(1)
	values[0][1] = "title"
	values[0][2] = "content"

	rows := NewRows(cols, values)

	mock.
		ExpectQuery("SELECT id, title, content FROM articles WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT id, title, content FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	cnt := 0
	for returnRows.Next() {
		var id int32
		var title string
		var content string
		err = returnRows.Scan(&id, &title, &content)
		if err != nil {
			t.Errorf("an error '%s' was not expected when scanning a row", err)
		}

		if id != 1 {
			t.Errorf("expected id to be 1, but got %d", id)
		}

		if title != "title" {
			t.Errorf("expected title to be title, but got %s", title)
		}

		if content != "content" {
			t.Errorf("expected content to be content, but got %s", content)
		}
		cnt++

		if cnt > 2 {
			t.Errorf("expected only 1 row, but got more")
			break
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestQueryExepectationsWithArgsAndRowsColumnTypes(t *testing.T) {
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := make([]ColumnType, 0)
	cols = append(cols, ColumnType{Type: "Int32", Name: "id"})
	cols = append(cols, ColumnType{Type: "String", Name: "title"})

	values := make([][]any, 1)
	values[0] = make([]any, 2)
	values[0][0] = int32(1)
	values[0][1] = "title"

	rows := NewRows(cols, values)

	mock.
		ExpectQuery("SELECT id, title FROM articles WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT id, title FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	cnt := 0
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
			t.Errorf("an error '%s' was not expected when scanning a row", err)
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
			t.Errorf("expected id to be 1, but got %d", id)
		}

		if title != "title" {
			t.Errorf("expected title to be title, but got %s", title)
		}

		cnt++

		if cnt > 2 {
			t.Errorf("expected only 1 row, but got more")
			break
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestExpectError(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1).WillReturnError(errors.New("some error"))

	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 1)
	if err == nil {
		t.Error("an error was expected when querying a statement")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUnfulfilledExpectation(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)

	if err := mock.ExpectationsWereMet(); err == nil {
		t.Errorf("an error was expected due to unfulfilled expectations")
	}
}

func TestQueryExpectationsWithDifferentDataTypes(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := make([]ColumnType, 0)
	cols = append(cols, ColumnType{Type: "Int32", Name: "id"})
	cols = append(cols, ColumnType{Type: "Float64", Name: "price"})
	cols = append(cols, ColumnType{Type: "String", Name: "description"})

	values := make([][]any, 1)
	values[0] = make([]any, 3)
	values[0][0] = int32(1)
	values[0][1] = float64(10.5)
	values[0][2] = "item"

	rows := NewRows(cols, values)

	mock.
		ExpectQuery("SELECT id, price, description FROM items WHERE id = ?").
		WithArgs(1).
		WillReturnRows(rows)

	returnedRows, err := mock.Query(context.Background(), "SELECT id, price, description FROM items WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	cnt := 0
	for returnedRows.Next() {
		var id int32
		var price float64
		var description string
		err = returnedRows.Scan(&id, &price, &description)
		if err != nil {
			t.Errorf("an error '%s' was not expected when scanning a row", err)
		}

		if id != 1 {
			t.Errorf("expected id to be 1, but got %d", id)
		}

		if price != 10.5 {
			t.Errorf("expected price to be 10.5, but got %f", price)
		}

		if description != "item" {
			t.Errorf("expected description to be item, but got %s", description)
		}
		cnt++

		if cnt > 2 {
			t.Errorf("expected only 1 row, but got more")
			break
		}
	}
}

func TestQueryExpectationsWithNoRows(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.
		ExpectQuery("SELECT id, title, content FROM articles WHERE id = ?").
		WithArgs(1).
		WillReturnRows(NewRows([]ColumnType{}, [][]any{}))

	returnRows, err := mock.Query(context.Background(), "SELECT id, title, content FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	if returnRows.Next() {
		t.Errorf("no rows were expected")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestMultipleQueries(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)
	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(2)

	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 2)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestStrictOrderingOfExpectations(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)
	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(2)

	// Querying out of order
	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 2)
	if err == nil {
		t.Error("an error was expected due to querying out of order")
	}
}

func TestUnexpectedQuery(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	// No expectation set for this query
	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 1)
	if err == nil {
		t.Error("an error was expected due to unexpected query")
	}
}

func TestCorrectNumberOfCalls(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)
	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)

	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	if err := mock.ExpectationsWereMet(); err == nil {
		t.Error("an error was expected due to incorrect number of calls")
	}
}

func TestArgumentMismatch(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)

	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 2)
	if err == nil {
		t.Error("an error was expected due to argument mismatch")
	}
}

func TestConnectionClose(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectClose()

	err = mock.Close()
	if err != nil {
		t.Errorf("an error '%s' was not expected when closing the connection", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestRowScanError(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := []ColumnType{
		{Type: "Int32", Name: "id"},
		{Type: "String", Name: "title"},
	}
	values := [][]any{
		{int32(1), "title"},
	}
	rows := NewRows(cols, values)

	mock.ExpectQuery("SELECT id, title FROM articles WHERE id = ?").WithArgs(1).WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT id, title FROM articles WHERE id = ?", 1)
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	var id int32
	// Trying to scan into fewer variables than columns in the result should result in an error
	if returnRows.Next() && returnRows.Scan(&id) == nil {
		t.Error("an error was expected due to row scan error")
	}
}

func TestUnmatchedExpectations(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)

	if err := mock.ExpectationsWereMet(); err == nil {
		t.Error("an error was expected due to unmatched expectations")
	}
}

func TestContextCancellation(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel the context

	_, err = mock.Query(ctx, "SELECT * FROM articles WHERE id = ?", 1)
	if err == nil {
		t.Error("an error was expected due to context cancellation")
	}
}

func TestQueryMultipleExpectedRows(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	rows := NewRows([]ColumnType{{Type: "Int32", Name: "id"}, {Type: "String", Name: "title"}},
		[][]any{{int32(1), "title1"}, {int32(2), "title2"}})

	mock.ExpectQuery("SELECT id, title FROM articles").WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT id, title FROM articles")
	if err != nil {
		t.Errorf("an error '%s' was not expected when querying a statement", err)
	}

	var count int
	for returnRows.Next() {
		var id int32
		var title string
		err = returnRows.Scan(&id, &title)
		if err != nil {
			t.Errorf("an error '%s' was not expected when scanning a row", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 rows, but got %d", count)
	}
}

func TestPrepareAndExecute(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative(nil)
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectPrepareBatch("INSERT INTO articles (title) VALUES (?)").
		ExpectSend()

	stmt, err := mock.PrepareBatch(context.Background(), "INSERT INTO articles (title) VALUES (?)")
	if err != nil {
		t.Errorf("an error '%s' was not expected when preparing a statement", err)
	}

	err = stmt.Send()
	if err != nil {
		t.Errorf("an error '%s' was not expected when executing a statement", err)
	}
}
