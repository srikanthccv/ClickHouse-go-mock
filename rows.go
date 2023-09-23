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

// This file is a copy of the original file from the clickhouse-go project.
// The original file can be found here:
// https://github.com/ClickHouse/clickhouse-go/blob/226a902d120aa46e3883fbf6a5a2667dfb9e90d2/clickhouse_rows.go

package mockhouse

import (
	"database/sql"
	"io"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type Rows struct {
	block     *proto.Block
	structMap *structMap
	colNames  []string
	colTypes  []driver.ColumnType
	values    [][]interface{}
	pos       int
	nextErr   map[int]error
	closeErr  error
}

func (r *Rows) Next() bool {
	return r.pos < len(r.values)
}

func (r *Rows) Scan(dest ...interface{}) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}
	if err := scan(r.block, r.pos, dest...); err != nil {
		return err
	}
	if err := r.nextErr[r.pos]; err != nil {
		return err
	}
	r.pos++
	return nil
}

func (r *Rows) ScanStruct(dest interface{}) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}
	if err := scan(r.block, r.pos, r.structMap, dest); err != nil {
		return err
	}
	if err := r.nextErr[r.pos]; err != nil {
		return err
	}
	r.pos++
	return nil
}

func (r *Rows) Totals(dest ...interface{}) error {
	return nil
}

func (r *Rows) Columns() []string {
	return r.colNames
}

func (r *Rows) ColumnTypes() []driver.ColumnType {
	return r.colTypes
}

func (r *Rows) Close() error {
	return r.closeErr
}

func (r *Rows) Err() error {
	return nil
}

type Row struct {
	err  error
	rows *Rows
}

func (r *Row) Err() error {
	return r.err
}

func (r *Row) ScanStruct(dest interface{}) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		r.rows.Close()
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.ScanStruct(dest); err != nil {
		return err
	}
	return r.rows.Close()
}

func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		r.rows.Close()
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	return r.rows.Close()
}

func getReflectType(typ string) reflect.Type {
	var reflectType reflect.Type
	switch typ {
	case "Int8":
		reflectType = reflect.TypeOf(int8(0))
	case "Int16":
		reflectType = reflect.TypeOf(int16(0))
	case "Int32":
		reflectType = reflect.TypeOf(int32(0))
	case "Int64":
		reflectType = reflect.TypeOf(int64(0))
	case "UInt8":
		reflectType = reflect.TypeOf(uint8(0))
	case "UInt16":
		reflectType = reflect.TypeOf(uint16(0))
	case "UInt32":
		reflectType = reflect.TypeOf(uint32(0))
	case "UInt64":
		reflectType = reflect.TypeOf(uint64(0))
	case "Float32":
		reflectType = reflect.TypeOf(float32(0))
	case "Float64":
		reflectType = reflect.TypeOf(float64(0))
	case "String":
		reflectType = reflect.TypeOf(string(""))
	case "FixedString":
		reflectType = reflect.TypeOf(string(""))
	case "Date":
		reflectType = reflect.TypeOf(string(""))
	case "DateTime":
		reflectType = reflect.TypeOf(string(""))
	case "UUID":
		reflectType = reflect.TypeOf(string(""))
	case "IPv4":
		reflectType = reflect.TypeOf(string(""))
	case "IPv6":
		reflectType = reflect.TypeOf(string(""))
	case "Array":
		reflectType = reflect.TypeOf([]interface{}{})
	case "Tuple":
		reflectType = reflect.TypeOf([]interface{}{})
	case "Nullable":
		reflectType = reflect.TypeOf(interface{}(nil))
	case "Nothing":
		reflectType = reflect.TypeOf(interface{}(nil))
	case "Enum8":
		reflectType = reflect.TypeOf(int8(0))
	case "Enum16":
		reflectType = reflect.TypeOf(int16(0))
	case "LowCardinality":
		reflectType = reflect.TypeOf(string(""))
	case "Decimal":
		reflectType = reflect.TypeOf(string(""))
	case "Decimal32":
		reflectType = reflect.TypeOf(int32(0))
	case "Decimal64":
		reflectType = reflect.TypeOf(int64(0))
	case "Decimal128":
		reflectType = reflect.TypeOf(string(""))
	case "Decimal256":
		reflectType = reflect.TypeOf(string(""))
	case "AggregateFunction":
		reflectType = reflect.TypeOf(string(""))
	case "Nested":
		reflectType = reflect.TypeOf(string(""))
	case "SimpleAggregateFunction":
		reflectType = reflect.TypeOf(string(""))
	case "TupleElement":
		reflectType = reflect.TypeOf(string(""))
	}
	return reflectType
}

func NewRows(columns map[string]column.Type, values [][]interface{}) *Rows {
	colNames := make([]string, 0, len(columns))
	colTypes := make([]driver.ColumnType, 0, len(columns))
	for name, typ := range columns {
		colNames = append(colNames, name)
		reflectType := getReflectType(string(typ))
		colTypes = append(colTypes, NewColumnType(name, string(typ), false, reflectType))
	}
	block := &proto.Block{}
	for name, typ := range columns {
		err := block.AddColumn(name, typ)
		if err != nil {
			panic(err)
		}
	}
	for _, row := range values {
		err := block.Append(row...)
		if err != nil {
			panic(err)
		}
	}
	return &Rows{
		block:     block,
		structMap: newStructMap(),
		colNames:  colNames,
		colTypes:  colTypes,
		values:    values,
	}
}
