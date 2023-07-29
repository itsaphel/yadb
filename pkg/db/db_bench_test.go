package db

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func BenchmarkGet(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	db := NewDatabase("wal")

	// Create database with 100 items
	for i := 0; i < 100; i++ {
		db.Set("key"+strconv.Itoa(i), "some random value")
	}

	// Benchmark
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get("key" + strconv.Itoa(rand.Intn(100)))
	}
}

func BenchmarkInsert(b *testing.B) {
	db := NewDatabase("wal")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set("key"+strconv.Itoa(i), "some random value")
	}
}
