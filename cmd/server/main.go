package main

import (
	"github.com/aleBranching/proglog/internal/server"
	"log"
)

func main() {

	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
