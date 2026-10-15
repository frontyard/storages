package simplefs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
	"github.com/darkweak/storages/simplefs"
	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"
)

const (
	byteKey        = "MyByteKey"
	nonExistentKey = "NonExistentKey"
	baseValue      = "My first data"
)

func getSimplefsInstance(t *testing.T) (core.Storer, error) {
	t.Helper()

	return simplefs.Factory(core.CacheProvider{Path: t.TempDir()}, zap.NewNop().Sugar(), 0)
}

// This test ensure that Simplefs options are override by the Souin configuration.
func TestCustomSimplefsConnectionFactory(t *testing.T) {
	instance, err := getSimplefsInstance(t)

	if nil != err {
		t.Error("Shouldn't have panic")
	}

	if nil == instance {
		t.Error("Simplefs should be instanciated")
	}
}

func TestSimplefsConnectionFactory(t *testing.T) {
	instance, err := getSimplefsInstance(t)

	if nil != err {
		t.Error("Shouldn't have panic")
	}

	if nil == instance {
		t.Error("Simplefs should be instanciated")
	}
}

func TestIShouldBeAbleToReadAndWriteDataInSimplefs(t *testing.T) {
	client, _ := getSimplefsInstance(t)

	_ = client.Set("Test", []byte(baseValue), time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	res := client.Get("Test")
	if len(res) == 0 {
		t.Errorf("Key %s should exist", baseValue)
	}

	if baseValue != string(res) {
		t.Errorf("%s not corresponding to %s", string(res), baseValue)
	}
}

func TestSimplefs_GetRequestInCache(t *testing.T) {
	client, _ := getSimplefsInstance(t)
	res := client.Get(nonExistentKey)

	if 0 < len(res) {
		t.Errorf("Key %s should not exist", nonExistentKey)
	}
}

func TestSimplefs_GetSetRequestInCache_OneByte(t *testing.T) {
	client, _ := getSimplefsInstance(t)
	_ = client.Set(byteKey, []byte("A"), time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	res := client.Get(byteKey)
	if len(res) == 0 {
		t.Errorf("Key %s should exist", byteKey)
	}

	if string(res) != "A" {
		t.Errorf("%s not corresponding to %v", res, 65)
	}
}

func TestSimplefs_SetRequestInCache_TTL(t *testing.T) {
	key := "MyEmptyKey"
	client, _ := getSimplefsInstance(t)
	value := []byte("Hello world")
	_ = client.Set(key, value, time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	newValue := client.Get(key)

	if len(newValue) != len(value) {
		t.Errorf("Key %s should be equals to %s, %s provided", key, value, newValue)
	}
}

func TestSimplefs_SetRequestInCache_Negative_TTL(t *testing.T) {
	client, _ := getSimplefsInstance(t)
	value := []byte("New value")
	_ = client.Set(byteKey, value, -1)

	time.Sleep(1 * time.Second)

	_ = client.Set(byteKey, value, time.Duration(20)*time.Second)
	time.Sleep(1 * time.Second)

	newValue := client.Get(byteKey)

	if len(newValue) != len(value) {
		t.Errorf("Key %s should be equals to %s, %s provided", byteKey, value, newValue)
	}
}

func TestSimplefs_DeleteRequestInCache(t *testing.T) {
	client, _ := getSimplefsInstance(t)
	client.Delete(byteKey)
	time.Sleep(1 * time.Second)

	if 0 < len(client.Get(byteKey)) {
		t.Errorf("Key %s should not exist", byteKey)
	}
}

func TestSimplefs_Init(t *testing.T) {
	client, _ := getSimplefsInstance(t)
	err := client.Init()

	if nil != err {
		t.Error("Impossible to init Simplefs provider")
	}
}

func TestSimplefs_EvictAfterXSeconds(t *testing.T) {
	client, _ := getSimplefsInstance(t)

	_ = client.Init()

	for i := range 10 {
		key := fmt.Sprintf("Test_%d", i)
		_ = client.SetMultiLevel(key, key, []byte(baseValue), http.Header{}, "", time.Second, key)

		time.Sleep(100 * time.Millisecond)
	}

	res := client.Get("Test_0")
	if len(res) != 0 {
		t.Errorf("Key %s should be evicted", "Test_0")
	}

	res = client.Get("Test_9")
	if len(res) == 0 {
		t.Errorf("Key %s should exist", "Test_9")
	}

	time.Sleep(3 * time.Second)
}

// TestSimplefs_SetMultiLevel_LargeValue tests that large values are not truncated
// when stored and retrieved via SetMultiLevel. This reproduces issue #41.
// See: https://github.com/darkweak/storages/issues/41
func TestSimplefs_SetMultiLevel_LargeValue(t *testing.T) {
	client, err := getSimplefsInstance(t)
	if err != nil {
		t.Fatalf("Failed to create simplefs instance: %v", err)
	}

	_ = client.Init()

	// Create a large value (5MB) to simulate the issue where large cached
	// responses were being truncated
	largeSize := 5 * 1024 * 1024

	largeValue := make([]byte, largeSize)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	key := "large_value_test"

	err = client.SetMultiLevel(key, key, largeValue, http.Header{}, "", time.Minute, key)
	if err != nil {
		t.Fatalf("Failed to set large value: %v", err)
	}

	// Retrieve and decompress
	compressed := client.Get(key)
	if compressed == nil {
		t.Fatal("Retrieved value is nil")
	}

	reader := lz4.NewReader(bytes.NewBuffer(compressed))

	decompressed := new(bytes.Buffer)
	if _, err := reader.WriteTo(decompressed); err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	retrieved := decompressed.Bytes()
	if len(retrieved) != len(largeValue) {
		t.Errorf("Data truncation: expected %d bytes, got %d bytes (%.2f%%)",
			len(largeValue), len(retrieved), float64(len(retrieved))/float64(len(largeValue))*100)
	}
}

// TestSimplefs_SetMultiLevel_DisabledCompression tests that values are written
// and returned uncompressed when disable_compression is set.
func TestSimplefs_SetMultiLevel_DisabledCompression(t *testing.T) {
	dir := t.TempDir()

	client, err := simplefs.Factory(core.CacheProvider{
		Path: dir,
		Configuration: map[string]interface{}{
			"disable_compression": true,
		},
	}, zap.NewNop().Sugar(), 0)
	if err != nil {
		t.Fatalf("Failed to create simplefs instance: %v", err)
	}

	_ = client.Init()

	value := []byte(baseValue)
	key := "uncompressed_key"

	if err = client.SetMultiLevel(key, key, value, http.Header{}, "", time.Minute, key); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	res := client.Get(key)
	if !bytes.Equal(res, value) {
		t.Errorf("Expected %q, got %q", value, res)
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read the storage directory: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("Expected 1 file in the storage directory, got %d", len(files))
	}

	stored, err := os.ReadFile(filepath.Join(dir, files[0].Name()))
	if err != nil {
		t.Fatalf("Failed to read the stored file: %v", err)
	}

	if !bytes.Equal(stored, value) {
		t.Errorf("Expected the file to contain %q, got %q", value, stored)
	}
}

// TestSimplefs_GetMultiLevel_DisabledCompression tests that a stored response
// written without compression can be elected and read back as an
// *http.Response by GetMultiLevel.
func TestSimplefs_GetMultiLevel_DisabledCompression(t *testing.T) {
	dir := t.TempDir()

	client, err := simplefs.Factory(core.CacheProvider{
		Path: dir,
		Configuration: map[string]interface{}{
			"disable_compression": true,
		},
	}, zap.NewNop().Sugar(), 0)
	if err != nil {
		t.Fatalf("Failed to create simplefs instance: %v", err)
	}

	_ = client.Init()

	body := []byte("hello world")
	resp := &http.Response{
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}

	var buf bytes.Buffer
	if err = resp.Write(&buf); err != nil {
		t.Fatalf("Failed to serialize response: %v", err)
	}

	key := "election_key"

	if err = client.SetMultiLevel(key, key, buf.Bytes(), http.Header{}, "", time.Minute, key); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://example.com/", nil)

	fresh, _ := client.GetMultiLevel(key, req, &core.Revalidator{})
	if fresh == nil {
		t.Fatal("Expected a fresh response")
	}

	read, err := io.ReadAll(fresh.Body)
	if err != nil {
		t.Fatalf("Failed to read the response body: %v", err)
	}

	if !bytes.Equal(read, body) {
		t.Errorf("Expected body %q, got %q", body, read)
	}
}

// TestSimplefs_DirectorySize_EvictsOldest tests that the oldest file-backed
// entries are evicted when directory_size is exceeded.
func TestSimplefs_DirectorySize_EvictsOldest(t *testing.T) {
	dir := t.TempDir()

	client, err := simplefs.Factory(core.CacheProvider{
		Path: dir,
		Configuration: map[string]interface{}{
			"directory_size":      "1KB",
			"disable_compression": true,
		},
	}, zap.NewNop().Sugar(), 0)
	if err != nil {
		t.Fatalf("Failed to create simplefs instance: %v", err)
	}

	_ = client.Init()

	payload := bytes.Repeat([]byte("x"), 700)

	if err = client.SetMultiLevel("Test_0", "Test_0", payload, http.Header{}, "", time.Minute, "Test_0"); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	if err = client.SetMultiLevel("Test_1", "Test_1", payload, http.Header{}, "", time.Minute, "Test_1"); err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	if res := client.Get("Test_0"); len(res) != 0 {
		t.Error("Key Test_0 should have been evicted to make room")
	}

	if res := client.Get("Test_1"); len(res) == 0 {
		t.Error("Key Test_1 should exist")
	}

	// The eviction callback runs on a separate goroutine, wait for the
	// evicted file to be removed.
	deadline := time.Now().Add(2 * time.Second)

	for {
		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("Failed to read the storage directory: %v", err)
		}

		if len(files) == 1 {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("Expected 1 file in the storage directory, got %d", len(files))
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// TestSimplefs_DirectorySize_TooSmall tests that a payload larger than
// directory_size is rejected instead of being written and evicted.
func TestSimplefs_DirectorySize_TooSmall(t *testing.T) {
	dir := t.TempDir()

	client, err := simplefs.Factory(core.CacheProvider{
		Path: dir,
		Configuration: map[string]interface{}{
			"directory_size":      "1KB",
			"disable_compression": true,
		},
	}, zap.NewNop().Sugar(), 0)
	if err != nil {
		t.Fatalf("Failed to create simplefs instance: %v", err)
	}

	_ = client.Init()

	payload := bytes.Repeat([]byte("x"), 2048)

	if err = client.SetMultiLevel("too_big", "too_big", payload, http.Header{}, "", time.Minute, "too_big"); err == nil {
		t.Error("Expected an error when the payload exceeds directory_size")
	}
}

// TestSimplefs_Init_RemovesOrphanedFiles tests that files not tracked by the
// in-memory index are deleted on Init.
func TestSimplefs_Init_RemovesOrphanedFiles(t *testing.T) {
	dir := t.TempDir()

	orphan := filepath.Join(dir, "orphaned-file")
	if err := os.WriteFile(orphan, []byte("stale data"), 0o644); err != nil {
		t.Fatalf("Failed to write the orphan file: %v", err)
	}

	client, err := simplefs.Factory(core.CacheProvider{Path: dir}, zap.NewNop().Sugar(), 0)
	if err != nil {
		t.Fatalf("Failed to create simplefs instance: %v", err)
	}

	if err = client.Init(); err != nil {
		t.Fatalf("Failed to init: %v", err)
	}

	if _, err = os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("Expected the orphan file to be removed on Init")
	}
}
