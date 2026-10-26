package series

import (
	"slices"
	"testing"
)

func keys(ids ...ID) []Key {
	out := make([]Key, len(ids))
	for i, id := range ids {
		out[i] = Key{ID: id, Day: 1}
	}
	return out
}

func TestCacheReportsOnlyMissingKeys(t *testing.T) {
	c := NewCache(100)
	c.Add(keys(1, 2))
	if got, want := c.Missing(keys(1, 2, 3)), keys(3); !slices.Equal(got, want) {
		t.Fatalf("Missing = %v, want %v", got, want)
	}
	// The same series on another day is another lookup row.
	if got := c.Missing([]Key{{ID: 1, Day: 2}}); len(got) != 1 {
		t.Fatalf("a known series on a new day must be missing, got %v", got)
	}
}

func TestCacheIsBounded(t *testing.T) {
	c := NewCache(4) // two generations of two keys
	for id := ID(1); id <= 10; id++ {
		c.Add(keys(id))
	}
	if n := c.Len(); n > 4 {
		t.Fatalf("Len = %d, want at most 4", n)
	}
	if got := c.Missing(keys(1)); len(got) != 1 {
		t.Fatal("the oldest key should have been evicted")
	}
	if got := c.Missing(keys(10)); len(got) != 0 {
		t.Fatal("the newest key should still be cached")
	}
}

func TestCacheKeepsActiveKeys(t *testing.T) {
	c := NewCache(4)
	c.Add(keys(1))
	for id := ID(2); id <= 20; id++ {
		if got := c.Missing(keys(1)); len(got) != 0 {
			t.Fatalf("key 1 is looked up continuously and must stay cached (evicted after %d adds)", id-2)
		}
		c.Add(keys(id))
	}
}

func TestDisabledCacheReportsEverythingMissing(t *testing.T) {
	for _, c := range []*Cache{NewCache(0), nil} {
		c.Add(keys(1))
		if got := c.Missing(keys(1)); len(got) != 1 {
			t.Fatalf("disabled cache returned %v", got)
		}
	}
}
