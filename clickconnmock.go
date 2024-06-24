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
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DATA-DOG/go-sqlmock"
)

type OpError struct { // taken from https://github.com/ClickHouse/clickhouse-go/blob/bfd9f33931482ddacadee5a899b760455b9268e6/clickhouse.go#L51
	Op         string
	ColumnName string
	Err        error
}

func (e *OpError) Error() string {
	switch err := e.Err.(type) {
	case *column.Error:
		return fmt.Sprintf("clickhouse [%s]: (%s %s) %s", e.Op, e.ColumnName, err.ColumnType, err.Err)
	case *column.ColumnConverterError:
		var hint string
		if len(err.Hint) != 0 {
			hint += ". " + err.Hint
		}
		return fmt.Sprintf("clickhouse [%s]: (%s) converting %s to %s is unsupported%s",
			err.Op, e.ColumnName,
			err.From, err.To,
			hint,
		)
	}
	return fmt.Sprintf("clickhouse [%s]: %s", e.Op, e.Err)
}

// ClickConnMockCommon interface serves to create expectations
// for any kind of database action in order to mock
// and test real database behavior.
type ClickConnMockCommon interface {
	// ExpectClose queues an expectation for this database
	// action to be triggered. the *ExpectedClose allows
	// to mock database response
	ExpectClose() *ExpectedClose

	// ExpectStats queues an expectation for this database
	// action to be triggered. the *ExpectedStats allows
	// to mock database response
	ExpectStats() *ExpectedStats

	// ExpectPing expects *driver.Conn.Ping to be called.
	// the *ExpectedPing allows to mock database response
	ExpectPing() *ExpectedPing

	// ExpectAsyncInsert expects *driver.Conn.AsyncInsert to be called.
	// the *ExpectedAsyncInsert allows to mock database response
	ExpectAsyncInsert(expectedSQL string, expectedWait bool) *ExpectedAsyncInsert

	// ExpectExec expects Exec() to be called with expectedSQL query.
	// the *ExpectedExec allows to mock database response
	ExpectExec(expectedSQL string) *ExpectedExec

	// ExpectPrepareBatch expects PrepareBatch() to be called with expectedSQL query.
	// the *ExpectedPrepareBatch allows to mock database response.
	// Note that you may expect Append() or AppendStruct() on the *ExpectedPrepareBatch
	// statement to prevent repeating expectedSQL
	ExpectPrepareBatch(expectedSQL string) *ExpectedPrepareBatch

	// ExpectQueryRow expects QueryRow() to be called with expectedSQL query.
	// the *ExpectedQuery allows to mock database response.
	ExpectQueryRow(expectedSQL string) *ExpectedQueryRow

	// ExpectQuery expects Query() to be called with expectedSQL query.
	// the *ExpectedQuery allows to mock database response.
	ExpectQuery(expectedSQL string) *ExpectedQuery

	// ExpectSelect expects Select() to be called with expectedSQL query.
	// the *ExpectedSelect allows to mock database response.
	ExpectSelect(expectedSQL string) *ExpectedSelect

	// ExpectServerVersion queues an expectation for this database
	// action to be triggered. the *ExpectedServerVersion allows
	// to mock database response
	ExpectServerVersion() *ExpectedServerVersion

	// ExpectContributors queues an expectation for this database
	// action to be triggered. the *ExpectedContributors allows
	// to mock database response
	ExpectContributors() *ExpectedContributors

	// ExpectationsWereMet checks whether all queued expectations
	// were met in order. If any of them was not met - an error is returned.
	ExpectationsWereMet() error

	// MatchExpectationsInOrder gives an option whether to match all
	// expectations in the order they were set or not.
	//
	// By default it is set to true. But if you use goroutines
	// to parallelize your query executation, that option may
	// be handy.
	//
	// This option may be turned on anytime during tests. As soon
	// as it is switched to false, expectations will be matched
	// in any order. Or otherwise if switched to true, any unmatched
	// expectations will be expected in order
	MatchExpectationsInOrder(bool)
}

type clickhousemock struct {
	ordered      bool
	dsn          string
	opened       int
	drv          *mockClickHouseDriver
	queryMatcher sqlmock.QueryMatcher
	monitorPings bool

	expected []expectation
}

var _ ClickConnMockCommon = (*clickhousemock)(nil)
var _ driver.Conn = (*clickhousemock)(nil)

func (c *clickhousemock) open(options *clickhouse.Options) (*clickhousemock, error) {
	if c.queryMatcher == nil {
		c.queryMatcher = sqlmock.QueryMatcherRegexp
	}
	return c, nil
}

func (c *clickhousemock) queryMatcherFunc() sqlmock.QueryMatcher {
	return c.queryMatcher
}

func (c *clickhousemock) MatchExpectationsInOrder(b bool) {
	c.ordered = b
}

func (c *clickhousemock) ExpectClose() *ExpectedClose {
	e := &ExpectedClose{}
	c.expected = append(c.expected, e)
	return e
}

// Close a mock database driver connection. It may or may not
// be called depending on the circumstances, but if it is called
// there must be an *ExpectedClose expectation satisfied.
// meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Close() error {
	c.drv.Lock()
	defer c.drv.Unlock()

	c.opened--
	if c.opened == 0 {
		delete(c.drv.conns, c.dsn)
	}

	var expected *ExpectedClose
	var fulfilled int
	var ok bool
	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if expected, ok = next.(*ExpectedClose); ok {
			break
		}

		next.Unlock()
		if c.ordered {
			return fmt.Errorf("call to database Close, was not expected, next expectation is: %s", next)
		}
	}

	if expected == nil {
		msg := "call to database Close was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return fmt.Errorf(msg)
	}

	expected.triggered = true
	expected.Unlock()
	return expected.err
}

func (c *clickhousemock) ExpectStats() *ExpectedStats {
	e := &ExpectedStats{}
	c.expected = append(c.expected, e)
	return e
}

// Stats meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Stats() driver.Stats {
	c.drv.Lock()
	defer c.drv.Unlock()

	var expected *ExpectedStats
	var fulfilled int
	var ok bool
	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if expected, ok = next.(*ExpectedStats); ok {
			break
		}

		next.Unlock()
		if c.ordered {
			panic(fmt.Errorf("call to database Stats, was not expected, next expectation is: %s", next))
		}
	}

	if expected == nil {
		msg := "call to database Stats was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		panic(fmt.Errorf(msg))
	}

	expected.triggered = true
	expected.Unlock()
	return expected.stats
}

// Ping meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Ping(ctx context.Context) error {
	if !c.monitorPings {
		return nil
	}

	c.drv.Lock()
	defer c.drv.Unlock()

	var expected *ExpectedPing
	var fulfilled int
	var ok bool
	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if expected, ok = next.(*ExpectedPing); ok {
			break
		}

		next.Unlock()
		if c.ordered {
			panic(fmt.Errorf("call to database Ping, was not expected, next expectation is: %s", next))
		}
	}

	if expected == nil {
		msg := "call to database Ping was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		panic(fmt.Errorf(msg))
	}

	expected.triggered = true
	expected.Unlock()
	return expected.err
}

func (c *clickhousemock) ExpectPing() *ExpectedPing {
	e := &ExpectedPing{}
	c.expected = append(c.expected, e)
	return e
}

func (c *clickhousemock) ExpectAsyncInsert(expectedSQL string, expectedWait bool) *ExpectedAsyncInsert {
	e := &ExpectedAsyncInsert{}
	e.expectSQL = expectedSQL
	e.expectWait = expectedWait
	c.expected = append(c.expected, e)
	return e
}

// AsyncInsert meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	ex, err := c.asyncInsert(ctx, query, wait)
	if ex != nil {
		select {
		case <-time.After(ex.delay):
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *clickhousemock) asyncInsert(ctx context.Context, query string, wait bool) (*ExpectedAsyncInsert, error) {
	var expected *ExpectedAsyncInsert
	var fulfilled int
	var ok bool

	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if c.ordered {
			if expected, ok = next.(*ExpectedAsyncInsert); ok {
				break
			}

			next.Unlock()
			return nil, fmt.Errorf("call to AsyncInsert statement with query '%s', was not expected, next expectation is: %s", query, next)
		}

		if pr, ok := next.(*ExpectedAsyncInsert); ok {
			if err := c.queryMatcher.Match(pr.expectSQL, query); err == nil {
				expected = pr
				break
			}
		}
		next.Unlock()
	}

	if expected == nil {
		msg := "call to AsyncInsert '%s' query was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return nil, fmt.Errorf(msg, query)
	}
	defer expected.Unlock()
	if err := c.queryMatcher.Match(expected.expectSQL, query); err != nil {
		return nil, fmt.Errorf("AsyncInsert: %v", err)
	}

	expected.triggered = true
	return expected, expected.err
}

func (c *clickhousemock) ExpectExec(expectedSQL string) *ExpectedExec {
	e := &ExpectedExec{}
	e.expectSQL = expectedSQL
	c.expected = append(c.expected, e)
	return e
}

// Exec meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Exec(ctx context.Context, query string, args ...any) error {
	c.drv.Lock()
	defer c.drv.Unlock()

	var expected *ExpectedExec
	var fulfilled int
	var ok bool
	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if expected, ok = next.(*ExpectedExec); ok {
			break
		}

		next.Unlock()
		if c.ordered {
			return fmt.Errorf("call to database Exec, was not expected, next expectation is: %s", next)
		}
	}

	if expected == nil {
		msg := "call to database Exec was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return fmt.Errorf(msg)
	}

	if err := c.queryMatcherFunc().Match(expected.expectSQL, query); err != nil {
		expected.Unlock()
		return fmt.Errorf("call to database Exec with unexpected query: %s", query)
	}

	expected.triggered = true
	expected.Unlock()
	return expected.err
}

func (c *clickhousemock) ExpectPrepareBatch(expectedSQL string) *ExpectedPrepareBatch {
	e := &ExpectedPrepareBatch{expectSQL: expectedSQL}
	c.expected = append(c.expected, e)
	return e
}

// PrepareBatch meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	ex, err := c.prepareBatch(ctx, query)
	if ex != nil {
		select {
		case <-time.After(ex.delay):
			if err != nil {
				return nil, err
			}
			return &batch{c, ex, query}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, err
}

func (c *clickhousemock) prepareBatch(ctx context.Context, query string) (*ExpectedPrepareBatch, error) {
	var expected *ExpectedPrepareBatch
	var fulfilled int
	var ok bool

	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if c.ordered {
			if expected, ok = next.(*ExpectedPrepareBatch); ok {
				break
			}

			next.Unlock()
			return nil, fmt.Errorf("call to Prepare statement with query '%s', was not expected, next expectation is: %s", query, next)
		}

		if pr, ok := next.(*ExpectedPrepareBatch); ok {
			if err := c.queryMatcher.Match(pr.expectSQL, query); err == nil {
				expected = pr
				break
			}
		}
		next.Unlock()
	}

	if expected == nil {
		msg := "call to Prepare '%s' query was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return nil, fmt.Errorf(msg, query)
	}
	defer expected.Unlock()
	if err := c.queryMatcherFunc().Match(expected.expectSQL, query); err != nil {
		return nil, err
	}

	expected.triggered = true
	return expected, expected.err
}

func (c *clickhousemock) ExpectQueryRow(expectedSQL string) *ExpectedQueryRow {
	e := &ExpectedQueryRow{}
	e.expectSQL = expectedSQL
	c.expected = append(c.expected, e)
	return e
}

// QueryRow meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ex, err := c.queryRow(ctx, query, args...)
	if ex != nil {
		ex.row.err = err
	}
	if ex != nil {
		select {
		case <-time.After(ex.delay):
		case <-ctx.Done():
		}
	}
	return ex.row
}

func (c *clickhousemock) queryRow(ctx context.Context, query string, args ...any) (*ExpectedQueryRow, error) {
	var expected *ExpectedQueryRow
	var fulfilled int
	var ok bool

	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if c.ordered {
			if expected, ok = next.(*ExpectedQueryRow); ok {
				break
			}

			next.Unlock()
			return nil, fmt.Errorf("call to QueryRow statement with query '%s', was not expected, next expectation is: %s", query, next)
		}

		if pr, ok := next.(*ExpectedQueryRow); ok {
			if err := c.queryMatcher.Match(pr.expectSQL, query); err == nil {
				expected = pr
				break
			}
		}
		next.Unlock()
	}

	if expected == nil {
		msg := "call to QueryRow '%s' query was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return nil, fmt.Errorf(msg, query)
	}
	defer expected.Unlock()
	if err := c.queryMatcher.Match(expected.expectSQL, query); err != nil {
		return nil, fmt.Errorf("QueryRow: %v", err)
	}

	expected.triggered = true
	return expected, expected.err
}

func (c *clickhousemock) ExpectQuery(expectedSQL string) *ExpectedQuery {
	e := &ExpectedQuery{}
	e.expectSQL = expectedSQL
	c.expected = append(c.expected, e)
	return e
}

// Query meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ex, err := c.query(ctx, query, args...)
	if ex != nil {
		select {
		case <-time.After(ex.delay):
			if err != nil {
				return nil, err
			}
			return ex.rows, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, err
}

func (c *clickhousemock) query(ctx context.Context, query string, args ...any) (*ExpectedQuery, error) {
	var expected *ExpectedQuery
	var fulfilled int
	var ok bool

	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if c.ordered {
			if expected, ok = next.(*ExpectedQuery); ok {
				break
			}

			next.Unlock()
			return nil, fmt.Errorf("call to Query statement with query '%s', was not expected, next expectation is: %s", query, next)
		}

		if pr, ok := next.(*ExpectedQuery); ok {
			if err := c.queryMatcher.Match(pr.expectSQL, query); err == nil {
				expected = pr
				break
			}
		}
		next.Unlock()
	}

	if expected == nil {
		msg := "call to Query '%s' query was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return nil, fmt.Errorf(msg, query)
	}
	defer expected.Unlock()
	if err := c.queryMatcher.Match(expected.expectSQL, query); err != nil {
		return nil, fmt.Errorf("Query: %v", err)
	}

	if err := expected.matchArgs(args); err != nil {
		return nil, fmt.Errorf("Query: '%s' arguments do not match: %s", query, err)
	}

	expected.triggered = true
	return expected, expected.err
}

func (c *clickhousemock) ExpectSelect(expectedSQL string) *ExpectedSelect {
	e := &ExpectedSelect{}
	e.expectSQL = expectedSQL
	c.expected = append(c.expected, e)
	return e
}

// Select meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Select(ctx context.Context, dest any, query string, args ...any) error {
	ex, err := c.selectQuery(ctx, query)
	if ex != nil {
		select {
		case <-time.After(ex.delay):
			if err != nil {
				return err
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *clickhousemock) selectQuery(ctx context.Context, query string) (*ExpectedSelect, error) {
	var expected *ExpectedSelect
	var fulfilled int
	var ok bool

	for _, next := range c.expected {
		next.Lock()
		if next.fulfilled() {
			next.Unlock()
			fulfilled++
			continue
		}

		if c.ordered {
			if expected, ok = next.(*ExpectedSelect); ok {
				break
			}

			next.Unlock()
			return nil, fmt.Errorf("call to Select statement with query '%s', was not expected, next expectation is: %s", query, next)
		}

		if pr, ok := next.(*ExpectedSelect); ok {
			if err := c.queryMatcher.Match(pr.expectSQL, query); err == nil {
				expected = pr
				break
			}
		}
		next.Unlock()
	}

	if expected == nil {
		msg := "call to Select '%s' query was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return nil, fmt.Errorf(msg, query)
	}
	defer expected.Unlock()
	if err := c.queryMatcher.Match(expected.expectSQL, query); err != nil {
		return nil, fmt.Errorf("Select: %v", err)
	}

	expected.triggered = true
	return expected, expected.err
}

func (c *clickhousemock) ExpectServerVersion() *ExpectedServerVersion {
	e := &ExpectedServerVersion{}
	c.expected = append(c.expected, e)
	return e
}

// ServerVersion returns the version of the database.
// meets http://golang.org/pkg/database/sql/driver/#Conn interface
func (c *clickhousemock) ServerVersion() (*driver.ServerVersion, error) {
	c.drv.Lock()
	defer c.drv.Unlock()

	var expected *ExpectedServerVersion
	var fulfilled int
	var ok bool
	for _, next := range c.expected {

		if next.fulfilled() {
			fulfilled++
			continue
		}

		if expected, ok = next.(*ExpectedServerVersion); ok {
			break
		}

		if c.ordered {
			return &driver.ServerVersion{}, fmt.Errorf("call to database ServerVersion, was not expected, next expectation is: %s", next)
		}
	}

	if expected == nil {
		msg := "call to database ServerVersion was not expected"
		if fulfilled == len(c.expected) {
			msg = "all expectations were already fulfilled, " + msg
		}
		return &driver.ServerVersion{}, fmt.Errorf(msg)
	}

	expected.triggered = true
	return &expected.version, expected.err
}

func (c *clickhousemock) ExpectContributors() *ExpectedContributors {
	e := &ExpectedContributors{}
	c.expected = append(c.expected, e)
	return e
}

// Contributors meets https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver#Conn interface
func (c *clickhousemock) Contributors() []string {
	c.drv.Lock()
	defer c.drv.Unlock()

	var expected *ExpectedContributors
	var fulfilled int
	var ok bool
	for _, next := range c.expected {

		if next.fulfilled() {
			fulfilled++
			continue
		}

		if expected, ok = next.(*ExpectedContributors); ok {
			break
		}

		if c.ordered {
			return []string{}
		}
	}

	if expected == nil {
		return []string{}
	}

	expected.triggered = true
	return expected.contributors
}

func (c *clickhousemock) ExpectationsWereMet() error {
	for _, e := range c.expected {
		e.Lock()
		fulfilled := e.fulfilled()
		e.Unlock()

		if !fulfilled {
			return fmt.Errorf("there is a remaining expectation which was not matched: %s", e)
		}

		// for expected prepared statement check whether it was closed if expected
		if prep, ok := e.(*ExpectedPrepareBatch); ok {
			if prep.mustBeSent && !prep.wasClosed {
				return fmt.Errorf("expected prepared statement to be closed, but it was not: %s", prep)
			}
		}

		// must check whether all expected queried rows are closed
		if query, ok := e.(*ExpectedQuery); ok {
			if query.rowsMustBeClosed && !query.rowsWereClosed {
				return fmt.Errorf("expected query rows to be closed, but it was not: %s", query)
			}
		}
	}
	return nil
}
