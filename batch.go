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

func (b *batch) Abort() error {
	return b.ex.abortErr
}

func (b *batch) Append(v ...any) error {
	for _, ex := range b.ex.expected {
		if ap, ok := ex.(*ExpectedAppend); ok && !ap.triggered {
			ap.triggered = true
			break
		}
	}
	return b.ex.appendErr
}

func (b *batch) AppendStruct(v any) error {
	return b.ex.appendStructErr
}

func (b *batch) Column(int) driver.BatchColumn {
	return batchcolumn{conn: b.conn, ex: b.ex, query: b.query}
}

func (b *batch) Flush() error {
	return b.ex.flushErr
}

func (b *batch) Send() error {
	return b.ex.sendErr
}

func (b *batch) IsSent() bool {
	return b.ex.mustBeSent
}

func (b *batch) Rows() int {
	return b.ex.rows
}

func (b *batch) Columns() []column.Interface {
	return nil
}
