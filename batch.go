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
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var _ driver.Batch = (*batch)(nil)

type batch struct {
	conn  *clickhousemock
	ex    *ExpectedPrepareBatch
	query string
}

type batchcolumn struct {
	conn  *clickhousemock
	ex    *ExpectedPrepareBatch
	query string
}

func (b batchcolumn) Append(any) error {
	return b.ex.appendErr
}

func (b batchcolumn) AppendRow(any) error {
	return b.ex.appendErr
}

// batchExpectation is a nested expectation of an *ExpectedPrepareBatch.
type batchExpectation interface {
	expectation
	markTriggered()
}

// triggerNext marks the first unfulfilled batch expectation of type T as triggered.
func triggerNext[T batchExpectation](ex *ExpectedPrepareBatch) (T, bool) {
	ex.Lock()
	defer ex.Unlock()

	for _, e := range ex.expected {
		t, ok := e.(T)
		if !ok {
			continue
		}

		t.Lock()
		if !t.fulfilled() {
			t.markTriggered()
			t.Unlock()

			return t, true
		}
		t.Unlock()
	}

	var zero T

	return zero, false
}

func (b *batch) Abort() error {
	triggerNext[*ExpectedAbort](b.ex)

	return b.ex.abortErr
}

func (b *batch) Append(v ...any) error {
	triggerNext[*ExpectedAppend](b.ex)

	return b.ex.appendErr
}

func (b *batch) AppendStruct(v any) error {
	triggerNext[*ExpectedAppendStruct](b.ex)

	return b.ex.appendStructErr
}

func (b *batch) Column(int) driver.BatchColumn {
	if ec, ok := triggerNext[*ExpectedColumn](b.ex); ok && ec.batchCoulmn != nil {
		return ec.batchCoulmn
	}

	return batchcolumn{conn: b.conn, ex: b.ex, query: b.query}
}

func (b *batch) Flush() error {
	triggerNext[*ExpectedFlush](b.ex)

	return b.ex.flushErr
}

func (b *batch) Send() error {
	triggerNext[*ExpectedSend](b.ex)

	b.ex.Lock()
	defer b.ex.Unlock()

	if b.ex.sendErr == nil {
		b.ex.wasSent = true
	}

	return b.ex.sendErr
}

func (b *batch) IsSent() bool {
	triggerNext[*ExpectedIsSent](b.ex)

	b.ex.Lock()
	defer b.ex.Unlock()

	return b.ex.wasSent || b.ex.isSent
}

func (b *batch) Rows() int {
	return b.ex.rows
}

func (b *batch) Columns() []column.Interface {
	return nil
}

func (b *batch) Close() error {
	triggerNext[*ExpectedBatchClose](b.ex)

	return b.ex.closeErr
}
