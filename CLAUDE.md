# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go mock of the `clickhouse-go/v2` native `driver.Conn` interface, modeled on `DATA-DOG/go-sqlmock` (it reuses sqlmock's `QueryMatcher` types). Module path is `github.com/srikanthccv/ClickHouse-go-mock`, but the Go package name is `mockhouse` (consumers usually alias it, e.g. `cmock`).

## Commands

```sh
go build -v ./...                        # what CI runs
go test -race -v ./...                   # what CI runs (-race needs cgo)
go test -run TestPrepareExpectations ./  # single test
golangci-lint run ./...                  # CI runs v2.10.0 with .golangci.yml
golangci-lint run --fix ./...            # autofixes wsl_v5/nlreturn/gofmt
```

- Lint is strict (`wsl_v5`, `nlreturn`, `lll` at 120 chars, `gocyclo`, etc.). The clickhouse-go copies (`scan.go`, `struct_map.go`, `rows_column_type.go`) are excluded from linters on purpose.
- `-race` needs cgo/gcc; without a local C toolchain, run it in Docker: `docker run --rm -v "$PWD:/src" -w /src golang:1.25 go test -race ./...`

- CI (`.github/workflows/go.yml`) runs on every branch and reads the Go version from `go.mod` (`go-version-file`).
- `examples/` is a **separate Go module** that depends on a published tag (`v0.4.0`) with no `replace` directive, so `./...` from the root does not build it and it does not exercise local changes. To run an example against local code, temporarily add `replace github.com/srikanthccv/ClickHouse-go-mock => ../` in `examples/go.mod`.

## Architecture

- **`driver.go`** — constructors. `NewClickHouseNative` uses `sqlmock.QueryMatcherEqual` (exact string match); `NewClickHouseWithQueryMatcher` lets callers pass e.g. `sqlmock.QueryMatcherRegexp`. Every mock is registered in a global, mutex-guarded pool keyed by a generated DSN.
- **`clickconnmock.go`** — `clickhousemock`, which implements both `driver.Conn` and the `ClickConnMockCommon` interface (the `Expect*` builder methods). Each `driver.Conn` method has a paired `ExpectX` that appends to `c.expected`, plus a private lookup helper (`query`, `queryRow`, `selectQuery`, `prepareBatch`, `asyncInsert`, …) that walks the expectation list:
  - `ordered` (default `true`, toggled via `MatchExpectationsInOrder`): the first unfulfilled expectation must be of the called type, otherwise it errors with "next expectation is: …".
  - unordered: the first unfulfilled expectation of the right type whose SQL (and args) match wins.
  - After a match: SQL is checked with `queryMatcher`, args via `matchArgs` (`Query`, always) or `matchArgsIfSet` (`Exec`/`QueryRow`/`Select`, only when `WithArgs` was called), `triggered` is set, and the public method honors `delay` with `ctx.Done()` cancellation outside the lock.
  - Locking: `c.mu` (per mock) guards `ordered`, `expected` and `unexpectedCalls`; each expectation has its own mutex, taken after `c.mu`. The global pool lock (`c.drv`) is only for the pool map.
  - Methods without an error return (`Stats`) record unexpected calls in `unexpectedCalls`, reported by `ExpectationsWereMet`.
  - `ExpectationsWereMet` also enforces `RowsWillBeClosed` (via `Rows.closed`), `WillBeSent` (successful `Send`) and that every nested batch expectation was triggered.
  New `driver.Conn` methods added upstream in clickhouse-go must be implemented here (compile-time assertion `var _ driver.Conn = (*clickhousemock)(nil)` will fail after a dependency bump otherwise).
- **`expectations.go`** — all `Expected*` types. Each embeds `commonExpectation` (mutex + `triggered` + `err`); query-like ones also embed `queryBasedExpectation` (`expectSQL`, `args`, delay). Builder methods return the receiver for chaining (`WithArgs`, `WillReturnError`, `WillDelayFor`, `WillReturnRows`, …). `ExpectedPrepareBatch` holds its own nested expectation list (`ExpectAppend`, `ExpectSend`, `ExpectFlush`, …) and the error values that `batch.go` returns.
- **`batch.go`** — `driver.Batch` / `driver.BatchColumn` implementation. Each method triggers the first unfulfilled nested expectation of its type (`triggerNext[T]`) and returns the errors/values configured on the parent `ExpectedPrepareBatch`. Calls without a matching nested expectation are allowed.
- **`rows.go`** — `NewRows` / `NewRow` build a real `proto.Block` from `[]ColumnType{Name, Type}` + values, so scanning goes through clickhouse-go's actual column decoders (unsupported types/values `panic` at construction). `WithTimezone` sets the block's server timezone (default UTC) for DateTime columns. The `reflectTypes` map (used by `getReflectType`) maps ClickHouse type strings to Go `ScanType`s for `ColumnTypes()` — extend it when adding column type support.
- **`scan.go`, `rows_column_type.go`, `struct_map.go`** — copied from clickhouse-go (see header comments for source commit) to provide `Scan`/`ScanStruct` and `ch:`-tag struct mapping; keep them close to upstream rather than refactoring.

Tests in `clickconnmock_test.go` are the primary usage documentation (per README).
