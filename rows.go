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
	values    [][]any
	pos       int
	nextErr   map[int]error
	closeErr  error
}

func (r *Rows) Next() bool {
	return r.pos < len(r.values)
}

func (r *Rows) Scan(dest ...any) error {
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

func (r *Rows) ScanStruct(dest any) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}

	// Based on implementation of rows.ScanStruct in clickhouse-go https://github.com/ClickHouse/clickhouse-go/blob/main/clickhouse_rows.go#L81
	values, err := r.structMap.Map("ScanStruct", r.Columns(), dest, true)
	if err != nil {
		return err
	}
	return r.Scan(values...)
}

func (r *Rows) Totals(dest ...any) error {
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

func (r *Row) ScanStruct(dest any) error {
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

func (r *Row) Scan(dest ...any) error {
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
		reflectType = reflect.TypeOf([]any{})
	case "Array(String)":
		reflectType = reflect.TypeOf([]string{})
	case "Array(Int64)":
		reflectType = reflect.TypeOf([]int64{})
	case "Array(Float64)":
		reflectType = reflect.TypeOf([]float64{})
	case "Array(Bool)":
		reflectType = reflect.TypeOf([]bool{})
	case "Tuple":
		reflectType = reflect.TypeOf([]any{})
	case "Nullable":
		reflectType = reflect.TypeOf(any(nil))
	case "Nothing":
		reflectType = reflect.TypeOf(any(nil))
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

type ColumnType struct {
	Name string
	Type column.Type
}

func NewRows(columns []ColumnType, values [][]any) *Rows {
	colNames := make([]string, 0, len(columns))
	colTypes := make([]driver.ColumnType, 0, len(columns))
	for _, col := range columns {
		colNames = append(colNames, col.Name)
		reflectType := getReflectType(string(col.Type))
		colTypes = append(colTypes, NewColumnType(col.Name, string(col.Type), false, reflectType))
	}
	block := &proto.Block{}
	for _, col := range columns {
		err := block.AddColumn(col.Name, col.Type)
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
