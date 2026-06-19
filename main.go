package main

import (
	"log"

	"feature-store/store"
)

func main() {
	log.Println("feature store starting")

	fs, err := store.NewFeatureStore("features.db")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer fs.Close()

	if err := fs.Set("user:123:age", "25"); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	log.Println("stored user:123:age = 25")

	val, err := fs.Get("user:123:age")
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	log.Printf("retrieved user:123:age = %s", val)

	if err := fs.Delete("user:123:age"); err != nil {
		log.Fatalf("delete failed: %v", err)
	}
	log.Println("deleted user:123:age")

	_, err = fs.Get("user:123:age")
	if err != nil {
		log.Printf("confirmed deleted: %v", err)
	}
}