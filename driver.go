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
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
)

var clickHousePool *mockClickHouseDriver

type mockClickHouseDriver struct {
	sync.Mutex
	counter int
	conns   map[string]*clickhousemock
	dsn     string
}

func init() {
	clickHousePool = &mockClickHouseDriver{
		conns: make(map[string]*clickhousemock),
	}
}

func (d *mockClickHouseDriver) Open(dsn string) (clickhouse.Conn, error) {
	d.Lock()
	defer d.Unlock()

	c, ok := d.conns[dsn]
	if !ok {
		return c, fmt.Errorf("expected a connection to be available, but it is not")
	}

	c.opened++
	return c, nil
}

// NewClickHouseNative creates clickhousemock database connection and a mock to manage expectations.
func NewClickHouseNative(options ...func(*clickhousemock) error) (*clickhousemock, error) {
	clickHousePool.Lock()
	dsn := fmt.Sprintf("clickhousemock_db_%d", clickHousePool.counter)
	clickHousePool.counter++

	cmock := &clickhousemock{dsn: dsn, drv: clickHousePool, ordered: true, queryMatcher: sqlmock.QueryMatcherEqual}
	clickHousePool.conns[dsn] = cmock
	clickHousePool.Unlock()

	for _, opt := range options {
		if err := opt(cmock); err != nil {
			return nil, err
		}
	}

	cmock.open(nil)

	return cmock, nil
}
