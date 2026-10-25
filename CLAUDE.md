# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go mock of the `clickhouse-go/v2` native `driver.Conn` interface, modeled on `DATA-DOG/go-sqlmock` (it reuses sqlmock's `QueryMatcher` types). Module path is `github.com/srikanthccv/ClickHouse-go-mock`, but the Go package name is `mockhouse` (consumers usually alias it, e.g. `cmock`).

## Commands

```sh
go build -v ./...                        # what CI runs
go test -v ./...                         # what CI runs
go test -run TestPrepareExpectations ./  # single test
go test -race ./...                      # tests use t.Parallel()
```

- CI (`.github/workflows/go.yml`) pins the Go version separately from `go.mod`; bump both together.
- `examples/` is a **separate Go module** that depends on a published tag (`v0.4.0`) with no `replace` directive, so `./...` from the root does not build it and it does not exercise local changes. To run an example against local code, temporarily add `replace github.com/srikanthccv/ClickHouse-go-mock => ../` in `examples/go.mod`.

## Architecture

- **`driver.go`** — constructors. `NewClickHouseNative` uses `sqlmock.QueryMatcherEqual` (exact string match); `NewClickHouseWithQueryMatcher` lets callers pass e.g. `sqlmock.QueryMatcherRegexp`. Every mock is registered in a global, mutex-guarded pool keyed by a generated DSN.
- **`clickconnmock.go`** — `clickhousemock`, which implements both `driver.Conn` and the `ClickConnMockCommon` interface (the `Expect*` builder methods). Each `driver.Conn` method has a paired `ExpectX` that appends to `c.expected`, plus a private lookup helper (`query`, `queryRow`, `selectQuery`, `prepareBatch`, `asyncInsert`, …) that walks the expectation list:
  - `ordered` (default `true`, toggled via `MatchExpectationsInOrder`): the first unfulfilled expectation must be of the called type, otherwise it errors with "next expectation is: …".
  - unordered: the first unfulfilled expectation of the right type whose SQL matches wins.
  - After a match: SQL is checked with `queryMatcher`, args via `matchArgs`, `triggered` is set, and the public method honors `delay` with `ctx.Done()` cancellation.
  - `ExpectationsWereMet` also enforces `RowsWillBeClosed` on queries and close-after-send on batches.
  New `driver.Conn` methods added upstream in clickhouse-go must be implemented here (compile-time assertion `var _ driver.Conn = (*clickhousemock)(nil)` will fail after a dependency bump otherwise).
- **`expectations.go`** — all `Expected*` types. Each embeds `commonExpectation` (mutex + `triggered` + `err`); query-like ones also embed `queryBasedExpectation` (`expectSQL`, `args`, delay). Builder methods return the receiver for chaining (`WithArgs`, `WillReturnError`, `WillDelayFor`, `WillReturnRows`, …). `ExpectedPrepareBatch` holds its own nested expectation list (`ExpectAppend`, `ExpectSend`, `ExpectFlush`, …) and the error values that `batch.go` returns.
- **`batch.go`** — `driver.Batch` / `driver.BatchColumn` implementation that simply returns the errors/values configured on the parent `ExpectedPrepareBatch`.
- **`rows.go`** — `NewRows` / `NewRow` build a real `proto.Block` from `[]ColumnType{Name, Type}` + values, so scanning goes through clickhouse-go's actual column decoders (unsupported types/values `panic` at construction). `WithTimezone` sets the block's server timezone (default UTC) for DateTime columns. `getReflectType` maps ClickHouse type strings to Go `ScanType`s for `ColumnTypes()` — extend it when adding column type support.
- **`scan.go`, `rows_column_type.go`, `struct_map.go`** — copied from clickhouse-go (see header comments for source commit) to provide `Scan`/`ScanStruct` and `db:`-tag struct mapping; keep them close to upstream rather than refactoring.

Tests in `clickconnmock_test.go` are the primary usage documentation (per README).
