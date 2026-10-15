package simplefs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darkweak/storages/core"
	"github.com/dustin/go-humanize"
	"github.com/jellydator/ttlcache/v3"
	"github.com/pierrec/lz4/v4"
)

// storedFile tracks a file written on disk for a cache key.
type storedFile struct {
	path string
	size int64
}

// Simplefs provider type.
type Simplefs struct {
	cache         *ttlcache.Cache[string, []byte]
	stale         time.Duration
	size          int
	path          string
	logger        core.Logger
	compression   bool
	directorySize int64
	actualSize    int64
	seq           atomic.Uint64
	initOnce      sync.Once
	stopOnce      sync.Once
	files         map[string]storedFile
	mu            sync.RWMutex
}

// Factory function create new Simplefs instance.
func Factory(simplefsCfg core.CacheProvider, logger core.Logger, stale time.Duration) (core.Storer, error) {
	storagePath := simplefsCfg.Path
	size := 0
	directorySize := int64(-1)
	compression := true

	simplefsConfiguration := simplefsCfg.Configuration
	if simplefsConfiguration != nil {
		if sfsconfig, ok := simplefsConfiguration.(map[string]interface{}); ok {
			if v, found := sfsconfig["size"]; found && v != nil {
				if val, ok := v.(int); ok && val > 0 {
					size = val
				} else if val, ok := v.(float64); ok && val > 0 {
					size = int(val)
				} else if val, ok := v.(string); ok {
					size, _ = strconv.Atoi(val)
				}
			}

			if v, found := sfsconfig["path"]; found && v != nil {
				if val, ok := v.(string); ok {
					storagePath = val
				}
			}

			if v, found := sfsconfig["directory_size"]; found && v != nil {
				if val, ok := v.(int64); ok && val > 0 {
					directorySize = val
				} else if val, ok := v.(float64); ok && val > 0 {
					directorySize = int64(val)
				} else if val, ok := v.(string); ok && val != "" {
					s, _ := humanize.ParseBytes(val)
					//nolint:gosec
					directorySize = int64(s)
				}
			}

			if v, found := sfsconfig["disable_compression"]; found && v != nil {
				if val, ok := v.(bool); ok {
					compression = !val
				} else if val, ok := v.(string); ok {
					if disabled, e := strconv.ParseBool(val); e == nil {
						compression = !disabled
					}
				}
			}
		}
	}

	var err error

	if storagePath == "" {
		logger.Info("No configuration path given, fallback to the current working directory.")

		storagePath, err = os.Getwd()
		if err != nil {
			logger.Errorf("Impossible to init the storage path: %#v", err)

			return nil, err
		}
	}

	cache := ttlcache.New(
		ttlcache.WithCapacity[string, []byte](uint64(size)),
	)

	if cache == nil {
		err = errors.New("impossible to initialize the simplefs storage")
		logger.Error(err)

		return nil, err
	}

	if err := os.MkdirAll(storagePath, 0o750); err != nil {
		logger.Errorf("impossible to create the storage directory: %#v", err)

		return nil, err
	}

	logger.Infof("Simplefs storage directory: %s", storagePath)

	store := &Simplefs{
		cache:         cache,
		compression:   compression,
		directorySize: directorySize,
		files:         make(map[string]storedFile),
		logger:        logger,
		path:          storagePath,
		size:          size,
		stale:         stale,
	}

	go cache.Start()

	return store, nil
}

// Name returns the storer name.
func (provider *Simplefs) Name() string {
	return "SIMPLEFS"
}

// Uuid returns an unique identifier.
func (provider *Simplefs) Uuid() string {
	return fmt.Sprintf("%s-%d", provider.path, provider.size)
}

// MapKeys method returns a map with the key and value.
func (provider *Simplefs) MapKeys(prefix string) map[string]string {
	keys := map[string]string{}

	provider.cache.Range(func(item *ttlcache.Item[string, []byte]) bool {
		if strings.HasPrefix(item.Key(), prefix) {
			k, _ := strings.CutPrefix(item.Key(), prefix)
			keys[k] = string(item.Value())
		}

		return true
	})

	return keys
}

// ListKeys method returns the list of existing keys.
func (provider *Simplefs) ListKeys() []string {
	return provider.cache.Keys()
}

// Get method returns the populated response if exists, empty response then.
func (provider *Simplefs) Get(key string) []byte {
	result := provider.cache.Get(key)
	if result == nil {
		return nil
	}

	provider.mu.RLock()
	entry, isFile := provider.files[key]
	provider.mu.RUnlock()

	if !isFile {
		return result.Value()
	}

	byteValue, err := os.ReadFile(entry.path)
	if err != nil {
		provider.logger.Debugf("Impossible to read the file %s from Simplefs: %#v", entry.path, err)

		return nil
	}

	return byteValue
}

// GetMultiLevel tries to load the key and check if one of linked keys is a fresh/stale candidate.
func (provider *Simplefs) GetMultiLevel(key string, req *http.Request, validator *core.Revalidator) (fresh *http.Response, stale *http.Response) {
	val := provider.cache.Get(core.MappingKeyPrefix + key)
	if val == nil {
		return fresh, stale
	}

	// core.MappingElection unconditionally decompresses values returned by Get,
	// so uncompressed storages must run the election themselves.
	if !provider.compression {
		return provider.mappingElection(val.Value(), req, validator)
	}

	fresh, stale, _ = core.MappingElection(provider, val.Value(), req, validator, provider.logger)

	return fresh, stale
}

// lz4FrameMagic is the magic number prefix of an LZ4 frame (0x184D2204,
// little-endian). Uncompressed payloads never start with these bytes.
var lz4FrameMagic = []byte{0x04, 0x22, 0x4D, 0x18}

// readStoredResponse reads a serialized response, decompressing it only when
// it carries an LZ4 frame header.
func readStoredResponse(data []byte, req *http.Request) (*http.Response, error) {
	var reader io.Reader = bytes.NewReader(data)

	if bytes.HasPrefix(data, lz4FrameMagic) {
		reader = lz4.NewReader(reader)
	}

	return http.ReadResponse(bufio.NewReader(reader), req)
}

// mappingElection mirrors core.electMapping for uncompressed storages.
func (provider *Simplefs) mappingElection(item []byte, req *http.Request, validator *core.Revalidator) (fresh *http.Response, stale *http.Response) {
	mapping, e := core.DecodeMapping(item)
	if e != nil {
		provider.logger.Errorf("Impossible to decode the mapping: %v", e)

		return fresh, stale
	}

	for keyName, keyItem := range mapping.GetMapping() {
		valid := true

		if req.Context().Value(core.DISABLE_VARY_CTX) == nil || !req.Context().Value(core.DISABLE_VARY_CTX).(bool) {
			for hname, hval := range keyItem.GetVariedHeaders() {
				if req.Header.Get(hname) != strings.Join(hval.GetHeaderValue(), ", ") {
					valid = false

					break
				}
			}
		}

		if !valid {
			continue
		}

		core.ValidateETagFromHeader(keyItem.GetEtag(), validator)

		if !validator.Matched {
			continue
		}

		if time.Since(keyItem.GetFreshTime().AsTime()) < 0 {
			if response := provider.Get(keyName); response != nil {
				if fresh, e = readStoredResponse(response, req); e != nil {
					provider.logger.Errorf("An error occurred while reading response for the key %s: %v", keyName, e)
				}

				return fresh, stale
			}
		}

		if time.Since(keyItem.GetStaleTime().AsTime()) < 0 {
			if response := provider.Get(keyName); response != nil {
				if stale, e = readStoredResponse(response, req); e != nil {
					provider.logger.Errorf("An error occurred while reading response for the key %s: %v", keyName, e)

					return fresh, stale
				}
			}
		}
	}

	return fresh, stale
}

// newFilePath returns a unique file path for the given key so an eviction of a
// previous write can never remove the current file.
func (provider *Simplefs) newFilePath(key string) string {
	return filepath.Join(provider.path, fmt.Sprintf("%s-%d", url.PathEscape(key), provider.seq.Add(1)))
}

// SetMultiLevel tries to store the key with the given value and update the mapping key to store metadata.
func (provider *Simplefs) SetMultiLevel(baseKey, variedKey string, value []byte, variedHeaders http.Header, etag string, duration time.Duration, realKey string) error {
	now := time.Now()

	payload := value

	if provider.compression {
		compressed := new(bytes.Buffer)
		writer := lz4.NewWriter(compressed)

		if _, err := writer.Write(value); err != nil {
			_ = writer.Close()

			provider.logger.Errorf("Impossible to compress the key %s into Simplefs, %v", variedKey, err)

			return err
		}

		if err := writer.Close(); err != nil {
			provider.logger.Errorf("Impossible to close the compressor for key %s into Simplefs, %v", variedKey, err)

			return err
		}

		payload = compressed.Bytes()
	}

	needed := int64(len(payload))
	if provider.directorySize > -1 && needed > provider.directorySize {
		return fmt.Errorf("impossible to store the key %s into Simplefs, the payload (%d bytes) exceeds directory_size (%d bytes)", variedKey, needed, provider.directorySize)
	}

	provider.freeSpace(needed)

	fp := provider.newFilePath(variedKey)
	//nolint:gosec
	if err := os.WriteFile(fp, payload, 0o644); err != nil {
		provider.logger.Errorf("Impossible to write the file %s from Simplefs: %#v", variedKey, err)

		return err
	}

	provider.mu.Lock()
	prev, existed := provider.files[variedKey]
	provider.files[variedKey] = storedFile{path: fp, size: needed}
	provider.actualSize += needed
	if existed {
		provider.actualSize -= prev.size
	}
	provider.mu.Unlock()

	_ = provider.cache.Set(variedKey, []byte(fp), duration+provider.stale)

	if existed && prev.path != fp {
		_ = os.Remove(prev.path)
	}

	mappingKey := core.MappingKeyPrefix + baseKey

	var previous []byte
	if item := provider.cache.Get(mappingKey); item != nil {
		previous = item.Value()
	}

	val, e := core.MappingUpdater(variedKey, previous, provider.logger, now, now.Add(duration), now.Add(duration+provider.stale), variedHeaders, etag, realKey)
	if e != nil {
		return e
	}

	_ = provider.cache.Set(mappingKey, val, ttlcache.NoTTL)

	return nil
}

// Set method will store the response in Simplefs provider.
func (provider *Simplefs) Set(key string, value []byte, duration time.Duration) error {
	provider.mu.Lock()
	prev, existed := provider.files[key]
	if existed {
		delete(provider.files, key)
		provider.actualSize -= prev.size
	}
	provider.mu.Unlock()

	_ = provider.cache.Set(key, value, duration)

	if existed {
		_ = os.Remove(prev.path)
	}

	return nil
}

// Delete method will delete the response in Simplefs provider if exists corresponding to key param.
func (provider *Simplefs) Delete(key string) {
	provider.cache.Delete(key)
}

// DeleteMany method will delete the responses in Simplefs provider if exists corresponding to the regex key param.
func (provider *Simplefs) DeleteMany(key string) {
	rgKey, e := regexp.Compile(key)
	if e != nil {
		return
	}

	var matched []string

	provider.cache.Range(func(item *ttlcache.Item[string, []byte]) bool {
		if rgKey.MatchString(item.Key()) {
			matched = append(matched, item.Key())
		}

		return true
	})

	for _, k := range matched {
		provider.cache.Delete(k)
	}
}

// Init method will.
func (provider *Simplefs) Init() error {
	var err error

	provider.initOnce.Do(func() {
		provider.cache.OnEviction(provider.onItemEvicted)
		err = provider.removeOrphanedFiles()
	})

	return err
}

// Reset method will reset or close provider.
func (provider *Simplefs) Reset() error {
	provider.cache.DeleteAll()
	provider.stopOnce.Do(provider.cache.Stop)

	provider.mu.Lock()
	leftover := make([]string, 0, len(provider.files))

	for _, f := range provider.files {
		leftover = append(leftover, f.path)
	}

	provider.files = make(map[string]storedFile)
	provider.actualSize = 0
	provider.mu.Unlock()

	for _, p := range leftover {
		_ = os.Remove(p)
	}

	return provider.removeOrphanedFiles()
}

// onItemEvicted removes the file associated to an evicted file-backed item and
// releases its size from the directory_size accounting.
func (provider *Simplefs) onItemEvicted(_ context.Context, _ ttlcache.EvictionReason, item *ttlcache.Item[string, []byte]) {
	evictedPath := string(item.Value())

	provider.mu.Lock()
	entry, ok := provider.files[item.Key()]
	if ok && entry.path != evictedPath {
		// The entry was overwritten before the eviction fired, the current
		// file is still in use and the previous one is removed by the writer.
		ok = false
	}

	if ok {
		delete(provider.files, item.Key())
		provider.actualSize -= entry.size
	}
	provider.mu.Unlock()

	if !ok {
		return
	}

	if err := os.Remove(evictedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		provider.logger.Errorf("impossible to remove the file %s: %#v", evictedPath, err)
	}
}

// removeOrphanedFiles deletes every file in the storage directory that is not
// tracked by the in-memory index.
func (provider *Simplefs) removeOrphanedFiles() error {
	provider.mu.RLock()
	tracked := make(map[string]struct{}, len(provider.files))

	for _, f := range provider.files {
		tracked[filepath.Base(f.path)] = struct{}{}
	}
	provider.mu.RUnlock()

	files, err := os.ReadDir(provider.path)
	if err != nil {
		provider.logger.Errorf("impossible to read the storage directory %s: %#v", provider.path, err)

		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if _, ok := tracked[f.Name()]; ok {
			continue
		}

		_ = os.Remove(filepath.Join(provider.path, f.Name()))
	}

	return nil
}

// freeSpace evicts the oldest file-backed items until needed bytes fit into
// the configured directory_size budget. Eviction callbacks are executed
// asynchronously by ttlcache, so victims are selected in a single pass from
// the least to the most recently used item.
func (provider *Simplefs) freeSpace(needed int64) {
	if provider.directorySize < 0 {
		return
	}

	// Reap expired entries first, they are dead weight.
	provider.cache.DeleteExpired()

	provider.mu.RLock()
	toFree := provider.actualSize + needed - provider.directorySize
	provider.mu.RUnlock()

	if toFree <= 0 {
		return
	}

	var victims []string

	var freed int64

	provider.cache.RangeBackwards(func(item *ttlcache.Item[string, []byte]) bool {
		provider.mu.RLock()
		entry, ok := provider.files[item.Key()]
		provider.mu.RUnlock()

		if !ok {
			return true
		}

		victims = append(victims, item.Key())
		freed += entry.size

		return freed < toFree
	})

	for _, victim := range victims {
		provider.cache.Delete(victim)
	}
}
