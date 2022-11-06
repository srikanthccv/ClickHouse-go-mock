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
)

func TestPrepareExpectations(t *testing.T) {
	t.Parallel()
	mock, err := NewClickHouseNative()
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
	}

	expectPrepareBatch := mock.ExpectPrepareBatch("INSERT INTO articles (id, title, content) VALUES (?, ?, ?)")

	append := expectPrepareBatch.ExpectAppend()
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
	mock, err := NewClickHouseNative()
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
