package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/swissy-dev/gocache"
	"github.com/swissy-dev/gocache/driver/memory"
)

type article struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func main() {
	cache, err := gocache.New(
		gocache.WithL1(memory.New(memory.WithMaxEntries(1000))),
		gocache.WithDefaultTTL(time.Minute),
		gocache.WithDefaultGrace(time.Hour),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cache.Close() }()

	ctx := context.Background()
	articles := cache.Namespace("articles")

	loaded, err := gocache.GetOrSet(ctx, articles, "42", func(ctx context.Context) (article, error) {
		return article{ID: 42, Title: "Caching in Go"}, nil
	}, gocache.WithTags("articles"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("loaded:", loaded.Title)

	cached, ok, err := gocache.Get[article](ctx, articles, "42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("cached:", cached.Title, ok)

	if err := cache.DeleteByTag(ctx, "articles"); err != nil {
		log.Fatal(err)
	}
	if _, ok, err = gocache.Get[article](ctx, articles, "42"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("after tag flush, found:", ok)

	if err := cache.Lock("rebuild", 10*time.Second).Do(ctx, func(ctx context.Context) error {
		fmt.Println("running under an atomic lock")
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
