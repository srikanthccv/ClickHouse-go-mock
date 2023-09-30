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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMockDriver(t *testing.T) {
	t.Parallel()
	_, err := NewClickHouseNative(nil)
	assert.NoError(t, err)
}

func TestInit(t *testing.T) {
	if clickHousePool == nil {
		t.Error("clickHousePool was not initialized")
	}

	if clickHousePool.conns == nil {
		t.Error("clickHousePool.conns was not initialized")
	}
}

func TestNewClickHouseNativeConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := NewClickHouseNative(nil)
			if err != nil {
				t.Errorf("an error '%s' was not expected when creating a new ClickHouseNative", err)
			}
		}()
	}
	wg.Wait()

	if len(clickHousePool.conns) < 10 {
		t.Errorf("expected >= 10 connections in pool, got %d", len(clickHousePool.conns))
	}

	if clickHousePool.counter < 10 {
		t.Errorf("expected counter to be >= 10, got %d", clickHousePool.counter)
	}
}
