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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExpectationsExpectedQuery(t *testing.T) {
	ex := ExpectedQuery{}
	ex.WillDelayFor(1).RowsWillBeClosed()
	assert.True(t, ex.delay > 0)
	assert.True(t, ex.rowsMustBeClosed)
}

func TestExpectationsExpectedPrepareBatch(t *testing.T) {
	ex := ExpectedPrepareBatch{}
	ex.ExpectAbort().WillReturnError(fmt.Errorf("error"))
	assert.True(t, ex.abortErr != nil)
}

func TestExpectedClose(t *testing.T) {
	t.Parallel()
	expectedClose := &ExpectedClose{}

	// Test WillReturnError method
	err := errors.New("some error")
	expectedClose.WillReturnError(err)

	if expectedClose.err != err {
		t.Errorf("Expected error to be set to %v, got %v", err, expectedClose.err)
	}

	// Test String method
	expectedString := "ExpectedClose => expecting database Close, which should return error: some error"
	if expectedClose.String() != expectedString {
		t.Errorf("Expected string representation to be %v, got %v", expectedString, expectedClose.String())
	}
}

func TestExpectedQuery(t *testing.T) {
	t.Parallel()
	expectedQuery := &ExpectedQuery{
		queryBasedExpectation: queryBasedExpectation{
			expectSQL: "SELECT * FROM table",
		},
	}

	// Test WithArgs method
	args := []any{1, "test"}
	expectedQuery.WithArgs(args...)
	if !reflect.DeepEqual(expectedQuery.args, args) {
		t.Errorf("Expected args to be %v, got %v", args, expectedQuery.args)
	}

	// Test WillReturnError method
	err := errors.New("some error")
	expectedQuery.WillReturnError(err)
	if expectedQuery.err != err {
		t.Errorf("Expected error to be %v, got %v", err, expectedQuery.err)
	}

	// Test WillDelayFor method
	duration := time.Second * 5
	expectedQuery.WillDelayFor(duration)
	if expectedQuery.delay != duration {
		t.Errorf("Expected delay to be %v, got %v", duration, expectedQuery.delay)
	}

	// Test WillReturnRows method
	rows := &Rows{}
	expectedQuery.WillReturnRows(rows)
	if expectedQuery.rows != rows {
		t.Errorf("Expected rows to be %v, got %v", rows, expectedQuery.rows)
	}
}

func TestExpectedPing(t *testing.T) {
	t.Parallel()
	expectedPing := &ExpectedPing{}

	// Test WillDelayFor method
	duration := time.Second * 5
	expectedPing.WillDelayFor(duration)
	if expectedPing.delay != duration {
		t.Errorf("Expected delay to be %v, got %v", duration, expectedPing.delay)
	}

	// Test WillReturnError method
	err := errors.New("some error")
	expectedPing.WillReturnError(err)
	if expectedPing.err != err {
		t.Errorf("Expected error to be %v, got %v", err, expectedPing.err)
	}

	// Test String method
	expectedString := "ExpectedPing => expecting database Ping, which should return error: some error"
	if expectedPing.String() != expectedString {
		t.Errorf("Expected string representation to be %v, got %v", expectedString, expectedPing.String())
	}
}
