package main

import (
	"context"
	"log"

	cmock "github.com/srikanthccv/ClickHouse-go-mock"
)

func main() {
	mock, err := cmock.NewClickHouseNative(nil)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(1)
	mock.ExpectQuery("SELECT * FROM articles WHERE id = ?").WithArgs(2)

	// Querying out of order
	_, err = mock.Query(context.Background(), "SELECT * FROM articles WHERE id = ?", 2)
	if err == nil {
		log.Fatal("an error was expected due to querying out of order")
	}
}
