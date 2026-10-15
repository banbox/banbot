package utils

import (
	"sync"
	"testing"
)

func TestGetSystemLanguageConcurrent(t *testing.T) {
	const workers = 32
	var wait sync.WaitGroup
	languages := make(chan string, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			languages <- GetSystemLanguage()
		}()
	}
	wait.Wait()
	close(languages)
	expected := GetSystemLanguage()
	if expected == "" {
		t.Fatal("system language was not initialized")
	}
	for language := range languages {
		if language != expected {
			t.Fatalf("concurrent system language = %q, want %q", language, expected)
		}
	}
}
