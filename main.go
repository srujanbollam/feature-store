package main

import (
	"log"

	"feature-store/api"
	"feature-store/store"
)

func main() {
	log.Println("feature store starting")

	fs, err := store.NewFeatureStore("features.db")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer fs.Close()

	server := api.NewServer(fs)
	log.Println("listening on :8080")
	if err := server.Start("8080"); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}