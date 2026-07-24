package null

import (
	"context"
	"testing"

	"github.com/swissy-dev/gocache"
)

func TestNullDriver(t *testing.T) {
	var d gocache.Driver = New()
	ctx := context.Background()
	if err := d.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := d.Get(ctx, "k"); ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if ok, err := d.Add(ctx, "k", []byte("v"), 0); !ok || err != nil {
		t.Fatalf("add ok=%v err=%v", ok, err)
	}
	if ok, err := d.Delete(ctx, "k"); ok || err != nil {
		t.Fatalf("delete ok=%v err=%v", ok, err)
	}
	if ok, err := d.DeleteIfEquals(ctx, "k", []byte("v")); ok || err != nil {
		t.Fatalf("dive ok=%v err=%v", ok, err)
	}
	if err := d.DeleteMany(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := d.ClearPrefix(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
