
# MockHouse

A mock library implementing support for [clickhouse-go/v2/lib/driver](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2/lib/driver)

## Example

```go
mock, err := NewClickHouseNative()
if err != nil {
	t.Errorf("an error '%s' was not expected when opening a stub database connection", err)
}

expectPrepareBatch := mock.ExpectPrepareBatch("INSERT INTO articles (id, title, content) VALUES (?, ?, ?)")

append := expectPrepareBatch.ExpectAppend()
if append == nil {
	t.Errorf("stmt was expected while creating a prepared statement")
}

var clickConn = mock // can be passed around to functions that require driver.Conn
batch, err := clickConn.PrepareBatch(context.Background(), "INSERT INTO articles (id, title, content) VALUES (?, ?, ?)")
if err != nil {
	t.Errorf("an error '%s' was not expected when preparing a batch statement", err)
}

batch.Append(1, "title", "content")

if err := mock.ExpectationsWereMet(); err != nil {
	t.Errorf("there were unfulfilled expectations: %s", err)
}
```
