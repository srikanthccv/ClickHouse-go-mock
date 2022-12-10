// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file is a copy of the original file from the clickhouse-go package.
// The original file is located at:
// https://github.com/ClickHouse/clickhouse-go/blob/226a902d120aa46e3883fbf6a5a2667dfb9e90d2/scan.go

package mockhouse

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func scan(block *proto.Block, row int, dest ...interface{}) error {
	columns := block.Columns
	if len(columns) != len(dest) {
		return &OpError{
			Op:  "Scan",
			Err: fmt.Errorf("expected %d destination arguments in Scan, not %d", len(columns), len(dest)),
		}
	}
	for i, d := range dest {
		if err := columns[i].ScanRow(d, row-1); err != nil {
			return &OpError{
				Err:        err,
				ColumnName: block.ColumnsNames()[i],
			}
		}
	}
	return nil
}
