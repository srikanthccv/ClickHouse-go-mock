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
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
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

	mock.ExpectQuery("SELECT id, title, content FROM articles WHERE id = ?")
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

	cols := make(map[string]column.Type)
	cols["id"] = "Int32"
	cols["title"] = "String"
	cols["content"] = "String"

	values := make([][]interface{}, 1)
	values[0] = make([]interface{}, 3)
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
