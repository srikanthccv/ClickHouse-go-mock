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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	clikhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// an expectation interface
type expectation interface {
	fulfilled() bool
	Lock()
	Unlock()
	String() string
}

// common expectation struct
// satisfies the expectation interface
type commonExpectation struct {
	sync.Mutex
	triggered bool
	err       error
}

func (e *commonExpectation) fulfilled() bool {
	return e.triggered
}

// ExpectedClose is used to manage *driver.Conn.Close expectation
// returned by *clickhousemock.ExpectClose.
type ExpectedClose struct {
	commonExpectation
}

// WillReturnError allows to set an error for *driver.Conn.Close action
func (e *ExpectedClose) WillReturnError(err error) *ExpectedClose {
	e.err = err
	return e
}

// String returns string representation
func (e *ExpectedClose) String() string {
	msg := "ExpectedClose => expecting database Close"
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}
	return msg
}

// ExpectedQuery is used to manage *driver.Conn.Query expectations.
// Returned by *clickhousemock.ExpectQuery.
type ExpectedQuery struct {
	queryBasedExpectation
	rows             *Rows
	delay            time.Duration
	rowsMustBeClosed bool
	rowsWereClosed   bool
}

// WithArgs will match given expected args to actual database query arguments.
// if at least one argument does not match, it will return an error. For specific
// arguments an clickhousemock.Argument interface can be used to match an argument.
func (e *ExpectedQuery) WithArgs(args ...any) *ExpectedQuery {
	e.args = args
	return e
}

// RowsWillBeClosed expects this query rows to be closed.
func (e *ExpectedQuery) RowsWillBeClosed() *ExpectedQuery {
	e.rowsMustBeClosed = true
	return e
}

// WillReturnError allows to set an error for expected database query
func (e *ExpectedQuery) WillReturnError(err error) *ExpectedQuery {
	e.err = err
	return e
}

// WillDelayFor allows to specify duration for which it will delay
// result. May be used together with Context
func (e *ExpectedQuery) WillDelayFor(duration time.Duration) *ExpectedQuery {
	e.delay = duration
	return e
}

// WillReturnRows allows to set rows for expected database query
func (e *ExpectedQuery) WillReturnRows(rows *Rows) *ExpectedQuery {
	e.rows = rows
	return e
}

// String returns string representation
func (e *ExpectedQuery) String() string {
	msg := "ExpectedQuery => expecting Query, QueryContext or QueryRow which:"
	msg += "\n  - matches sql: '" + e.expectSQL + "'"

	if len(e.args) == 0 {
		msg += "\n  - is without arguments"
	} else {
		msg += "\n  - is with arguments:\n"
		for i, arg := range e.args {
			msg += fmt.Sprintf("    %d - %+v\n", i, arg)
		}
		msg = strings.TrimSpace(msg)
	}

	if e.rows != nil {
		msg += fmt.Sprintf("\n  - %v", e.rows)
	}

	if e.err != nil {
		msg += fmt.Sprintf("\n  - should return error: %s", e.err)
	}

	return msg
}

// ExpectedExec is used to manage *driver.Conn.Exec expectations.
// Returned by *clickhousemock.ExpectExec.
type ExpectedExec struct {
	queryBasedExpectation
	delay time.Duration
}

// WithArgs will match given expected args to actual database exec operation arguments.
// if at least one argument does not match, it will return an error. For specific
// arguments an clickhousemock.Argument interface can be used to match an argument.
func (e *ExpectedExec) WithArgs(args ...any) *ExpectedExec {
	e.args = args
	return e
}

// WillReturnError allows to set an error for expected database exec action
func (e *ExpectedExec) WillReturnError(err error) *ExpectedExec {
	e.err = err
	return e
}

// WillDelayFor allows to specify duration for which it will delay
// result. May be used together with Context
func (e *ExpectedExec) WillDelayFor(duration time.Duration) *ExpectedExec {
	e.delay = duration
	return e
}

// String returns string representation
func (e *ExpectedExec) String() string {
	msg := "ExpectedExec => expecting Exec or ExecContext which:"
	msg += "\n  - matches sql: '" + e.expectSQL + "'"

	if len(e.args) == 0 {
		msg += "\n  - is without arguments"
	} else {
		msg += "\n  - is with arguments:\n"
		var margs []string
		for i, arg := range e.args {
			margs = append(margs, fmt.Sprintf("    %d - %+v", i, arg))
		}
		msg += strings.Join(margs, "\n")
	}

	if e.err != nil {
		msg += fmt.Sprintf("\n  - should return error: %s", e.err)
	}

	return msg
}

// query based expectation
// adds a query matching logic
type queryBasedExpectation struct {
	commonExpectation
	expectSQL string
	args      []any
}

func (e *queryBasedExpectation) matchArgs(args []any) error {
	if len(e.args) == 0 && len(args) == 0 {
		return nil
	}

	if len(e.args) != len(args) {
		return fmt.Errorf("expected %d arguments, got %d", len(e.args), len(args))
	}

	for i, arg := range e.args {
		if err := matchArg(arg, args[i]); err != nil {
			return fmt.Errorf("argument %d: %s", i, err)
		}
	}

	return nil
}

func matchArg(expected, actual any) error {
	if expected == nil {
		return nil
	}

	if reflect.DeepEqual(expected, actual) {
		return nil
	}

	return fmt.Errorf("expected %v (%T), got %v (%T)", expected, expected, actual, actual)
}

// ExpectedPing is used to manage *driver.Conn.Ping expectations.
// Returned by *clickhousemock.ExpectPing.
type ExpectedPing struct {
	commonExpectation
	delay time.Duration
}

// WillDelayFor allows to specify duration for which it will delay result. May
// be used together with Context.
func (e *ExpectedPing) WillDelayFor(duration time.Duration) *ExpectedPing {
	e.delay = duration
	return e
}

// WillReturnError allows to set an error for expected database ping
func (e *ExpectedPing) WillReturnError(err error) *ExpectedPing {
	e.err = err
	return e
}

// String returns string representation
func (e *ExpectedPing) String() string {
	msg := "ExpectedPing => expecting database Ping"
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}
	return msg
}

// ExpectedPrepareBatch is used to manage *driver.Conn.PrepareBatch expectations.
// Returned by *clickhousemock.ExpectedPrepareBatch.
type ExpectedPrepareBatch struct {
	commonExpectation
	expected        []expectation
	expectSQL       string
	abortErr        error
	appendErr       error
	appendStructErr error
	flushErr        error
	sendErr         error
	mustBeSent      bool
	wasClosed       bool
	delay           time.Duration
	rows            int
	isSent          bool
}

// WillReturnError allows to set an error for the expected *driver.Conn.PrepareBatch action.
func (e *ExpectedPrepareBatch) WillReturnError(err error) *ExpectedPrepareBatch {
	e.err = err
	return e
}

// WillDelayFor allows to specify duration for which it will delay
// result. May be used together with Context
func (e *ExpectedPrepareBatch) WillDelayFor(duration time.Duration) *ExpectedPrepareBatch {
	e.delay = duration
	return e
}

// WillBeSent expects this prepared batch statement to
// be set with sent.
func (e *ExpectedPrepareBatch) WillBeSent() *ExpectedPrepareBatch {
	e.mustBeSent = true
	return e
}

// WillReturnRows expects this prepared batch statement to
// be set with rows.
func (e *ExpectedPrepareBatch) WillReturnRows(rows int) *ExpectedPrepareBatch {
	e.rows = rows
	return e
}

type ExpectedAbort struct {
	commonExpectation
	expBatch  *ExpectedPrepareBatch
	expectSQL string
}

// WillReturnError allows to set an error for the expected *batch.Abort action.
func (e *ExpectedAbort) WillReturnError(err error) *ExpectedAbort {
	e.expBatch.abortErr = err
	e.err = err
	return e
}

func (e *ExpectedAbort) String() string {
	return fmt.Sprintf("Abort(%s)", e.expectSQL)
}

// ExpectAbort allows to expect Abort() on this prepared batch statement.
func (e *ExpectedPrepareBatch) ExpectAbort() *ExpectedAbort {
	eq := &ExpectedAbort{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

type ExpectedAppend struct {
	commonExpectation
	expBatch  *ExpectedPrepareBatch
	expectSQL string
}

// WillReturnError allows to set an error for the expected *batch.Append action.
func (e *ExpectedAppend) WillReturnError(err error) *ExpectedAppend {
	e.err = err
	e.expBatch.appendErr = err
	return e
}

func (e *ExpectedAppend) String() string {
	return fmt.Sprintf("Append(%s)", e.expectSQL)
}

// ExpectAppend allows to expect Append() on this prepared batch statement.
// This method is convenient in order to prevent duplicating sql query string matching.
func (e *ExpectedPrepareBatch) ExpectAppend() *ExpectedAppend {
	eq := &ExpectedAppend{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

type ExpectedAppendStruct struct {
	commonExpectation
	expBatch  *ExpectedPrepareBatch
	expectSQL string
}

// WillReturnError allows to set an error for the expected *batch.AppendStruct action.
func (e *ExpectedAppendStruct) WillReturnError(err error) *ExpectedAppendStruct {
	e.err = err
	e.expBatch.appendStructErr = err
	return e
}

func (e *ExpectedAppendStruct) String() string {
	return fmt.Sprintf("AppendStruct(%s)", e.expectSQL)
}

// ExpectAppendStruct allows to expect AppendStruct() on this prepared batch statement.
func (e *ExpectedPrepareBatch) ExpectAppendStruct() *ExpectedAppendStruct {
	eq := &ExpectedAppendStruct{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

type ExpectedColumn struct {
	commonExpectation
	expectSQL   string
	expBatch    *ExpectedPrepareBatch
	batchCoulmn clikhouseDriver.BatchColumn
}

// WillReturnBatchColumn allows to set an error for the expected *batch.Column action.
func (e *ExpectedColumn) WillReturnBatchColumn(batchcolumn clikhouseDriver.BatchColumn) *ExpectedColumn {
	e.batchCoulmn = batchcolumn
	return e
}

func (e *ExpectedColumn) String() string {
	return fmt.Sprintf("Column(%s)", e.expectSQL)
}

// ExpectColumn allows to expect Column() on this prepared batch statement.
func (e *ExpectedPrepareBatch) ExpectColumn() *ExpectedColumn {
	eq := &ExpectedColumn{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

type ExpectedFlush struct {
	commonExpectation
	expBatch  *ExpectedPrepareBatch
	expectSQL string
}

// WillReturnError allows to set an error for the expected *batch.Flush action.
func (e *ExpectedFlush) WillReturnError(err error) *ExpectedFlush {
	e.err = err
	e.expBatch.flushErr = err
	return e
}

func (e *ExpectedFlush) String() string {
	return fmt.Sprintf("Flush(%s)", e.expectSQL)
}

// ExpectFlush allows to expect Flush() on this prepared batch statement.
func (e *ExpectedPrepareBatch) ExpectFlush() *ExpectedFlush {
	eq := &ExpectedFlush{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

type ExpectedSend struct {
	commonExpectation
	expBatch  *ExpectedPrepareBatch
	expectSQL string
}

// WillReturnError allows to set an error for the expected *batch.Send action.
func (e *ExpectedSend) WillReturnError(err error) *ExpectedSend {
	e.err = err
	e.expBatch.sendErr = err
	return e
}

func (e *ExpectedSend) String() string {
	return fmt.Sprintf("Send(%s)", e.expectSQL)
}

// ExpectSend allows to expect Send() on this prepared batch statement.
func (e *ExpectedPrepareBatch) ExpectSend() *ExpectedSend {
	eq := &ExpectedSend{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

type ExpectedIsSent struct {
	commonExpectation
	expectSQL string
	expBatch  *ExpectedPrepareBatch
	isSent    bool
}

// WillReturnBool allows to set an bool for the expected *batch.IsSent action.
func (e *ExpectedIsSent) WillReturnBool(b bool) *ExpectedIsSent {
	e.isSent = b
	e.expBatch.isSent = b
	e.expBatch.mustBeSent = b
	return e
}

func (e *ExpectedIsSent) String() string {
	return fmt.Sprintf("IsSent(%s)", e.expectSQL)
}

// ExpectIsSent allows to expect IsSent() on this prepared batch statement.
func (e *ExpectedPrepareBatch) ExpectIsSent() *ExpectedIsSent {
	eq := &ExpectedIsSent{}
	eq.expectSQL = e.expectSQL
	eq.expBatch = e
	e.expected = append(e.expected, eq)
	return eq
}

// String returns string representation
func (e *ExpectedPrepareBatch) String() string {
	msg := "ExpectedPrepareBatch => expecting PrepareBatch statement which:"
	msg += "\n  - matches sql: '" + e.expectSQL + "'"

	if e.err != nil {
		msg += fmt.Sprintf("\n  - should return error: %s", e.err)
	}

	if e.abortErr != nil {
		msg += fmt.Sprintf("\n  - should return error on Abort: %s", e.abortErr)
	}

	if e.appendErr != nil {
		msg += fmt.Sprintf("\n  - should return error on Append: %s", e.appendErr)
	}

	if e.appendStructErr != nil {
		msg += fmt.Sprintf("\n  - should return error on AppendStruct: %s", e.appendStructErr)
	}

	if e.flushErr != nil {
		msg += fmt.Sprintf("\n  - should return error on Flush: %s", e.flushErr)
	}

	if e.sendErr != nil {
		msg += fmt.Sprintf("\n  - should return error on Send: %s", e.sendErr)
	}

	if e.delay != 0 {
		msg += fmt.Sprintf("\n  - should delay for: %s", e.delay)
	}

	if e.mustBeSent {
		msg += "\n  - should be sent"
	}

	return msg
}

type ExpectedStats struct {
	commonExpectation
	expectSQL string
	stats     clikhouseDriver.Stats
}

// WillReturnStats allows to set an error for the expected *Conn.Stats action.
func (e *ExpectedStats) WillReturnStats(stats clikhouseDriver.Stats) *ExpectedStats {
	e.stats = stats
	return e
}

func (e *ExpectedStats) String() string {
	return fmt.Sprintf("Stats(%s)", e.expectSQL)
}

type ExpectedAsyncInsert struct {
	commonExpectation
	expectSQL  string
	expectWait bool
	wait       bool
	delay      time.Duration
}

// WillReturnError allows to set an error for the expected *Conn.AsyncInsert action.
func (e *ExpectedAsyncInsert) WillReturnError(err error) *ExpectedAsyncInsert {
	e.err = err
	return e
}

// WillWait allows to set a wait for the expected *Conn.AsyncInsert action.
func (e *ExpectedAsyncInsert) WillWait() *ExpectedAsyncInsert {
	e.wait = true
	return e
}

// WillDelay allows to set a delay for the expected *Conn.AsyncInsert action.
func (e *ExpectedAsyncInsert) WillDelay(d time.Duration) *ExpectedAsyncInsert {
	e.delay = d
	return e
}

func (e *ExpectedAsyncInsert) String() string {
	return fmt.Sprintf("AsyncInsert(%s)", e.expectSQL)
}

type ExpectedQueryRow struct {
	commonExpectation
	expectSQL string
	row       *Row
	delay     time.Duration
}

// WillReturnRow allows to set a row for the expected *Conn.QueryRow action.
func (e *ExpectedQueryRow) WillReturnRow(row *Row) *ExpectedQueryRow {
	e.row = row
	return e
}

func (e *ExpectedQueryRow) WillReturnError(err error) *ExpectedQueryRow {
	e.row.err = err
	e.err = err
	return e
}

// WillDelay allows to set a delay for the expected *Conn.QueryRow action.
func (e *ExpectedQueryRow) WillDelay(d time.Duration) *ExpectedQueryRow {
	e.delay = d
	return e
}

func (e *ExpectedQueryRow) String() string {
	return fmt.Sprintf("QueryRow(%s)", e.expectSQL)
}

type ExpectedSelect struct {
	commonExpectation
	expectSQL string
	delay     time.Duration
	rows      *Rows
}

func (e *ExpectedSelect) WillReturnRows(rows *Rows) *ExpectedSelect {
	e.rows = rows
	return e
}

// WillReturnError allows to set an error for the expected *Conn.Select action.
func (e *ExpectedSelect) WillReturnError(err error) *ExpectedSelect {
	e.err = err
	return e
}

// WillDelay allows to set a delay for the expected *Conn.Select action.
func (e *ExpectedSelect) WillDelay(d time.Duration) *ExpectedSelect {
	e.delay = d
	return e
}

func (e *ExpectedSelect) String() string {
	return fmt.Sprintf("Select(%s)", e.expectSQL)
}

type ExpectedServerVersion struct {
	commonExpectation
	version proto.ServerHandshake
}

// WillReturnVersion allows to set an error for the expected *Conn.ServerVersion action.
func (e *ExpectedServerVersion) WillReturnVersion(version proto.ServerHandshake) *ExpectedServerVersion {
	e.version = version
	return e
}

func (e *ExpectedServerVersion) String() string {
	return "ServerVersion() - Mocked"
}

type ExpectedContributors struct {
	commonExpectation
	contributors []string
}

// WillReturnContributors allows to set an error for the expected *Conn.Contributors action.
func (e *ExpectedContributors) WillReturnContributors(contributors ...string) *ExpectedContributors {
	e.contributors = contributors
	return e
}

func (e *ExpectedContributors) String() string {
	return "Contributors() - Mocked"
}
