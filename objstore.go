package objstore

import (
	"io"
	"time"
)

// Options represent configuration options that may be passed to enable
// or disable certain functionality.
type Options uint32

const (
	// Compressed indicates that the implementation should use compression
	// or assume object was compressed and would like the object
	// decompressed.
	Compressed Options = 1 << (32 - 1 - iota)
)

// ObjStore is a wrapper around an object store so we can change
// implementations easily especially during tests.
type ObjStore interface {
	// GetPresignedURL generates a URL that can be consumed by a third
	// party for retrieving the object specified by its bucket and key. The
	// URL is valid for expire minutes.
	GetPresignedURL(bucket, key string, expire time.Duration) (string, error)
	// Upload the the contents of body to the specified bucket and key.
	Upload(body io.Reader, bucket, key string, opts Options) error
	// Download retrieves the key from the specified bucket.
	Download(bucket, key string, opts Options) ([]byte, error)
}

// UseCompression returns true if the ObjStore implementation should use
// compression. This may mean different things in different contexts, like
// compress or decompress for upload and download respectively.
func UseCompression(opts Options) bool {
	return opts&Compressed != 0
}
