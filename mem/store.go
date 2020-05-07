package mem

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/Synthesis-AI-Dev/objstore"
)

// Store is a mock ObjStore implementation that can be used for tests. It "uploads"
// and "downloads" to an in-memory filesystem.
type Store struct {
	fs afero.Fs
}

// GetPresignedURL generates a fake URL.
func (s *Store) GetPresignedURL(bucket, key string, expire time.Duration) (string, error) {
	return fmt.Sprintf("https://%s/%s", bucket, key), nil
}

func keyPath(bucket, key string) string {
	return fmt.Sprintf("%s/%s", bucket, key)
}

// Upload implements the ObjStore.Upload interface. It stores the data in
// memory in such a way it can be retrieved by Download.
func (s *Store) Upload(body io.Reader, bucket, key string, opts objstore.Options) error {
	if err := s.fs.Mkdir(bucket, 0755); err != nil {
		if strings.Index(err.Error(), "already exists") == -1 {
			// bucket name is never unique, so this is ok
			return err
		}
	}
	b, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}

	if objstore.UseCompression(opts) {
		var buff bytes.Buffer
		zWriter := zlib.NewWriter(&buff)
		if _, err := zWriter.Write(b); err != nil {
			return err
		}
		zWriter.Close()
		b = buff.Bytes()
	}
	return afero.WriteFile(s.fs, keyPath(bucket, key), b, 0644)
}

// Download implements the ObjStore.Download interface. It retrieves the data
// from an in-memory file store.
func (s *Store) Download(ctx context.Context, bucket, key string, opts objstore.Options) ([]byte, error) {
	fileName := keyPath(bucket, key)
	_, err := s.fs.Stat(fileName)
	if err != nil {
		return nil, err
	}
	buff, err := afero.ReadFile(s.fs, fileName)
	if err != nil {
		return nil, err
	}
	if objstore.UseCompression(opts) {
		body, err := zlib.NewReader(bytes.NewReader(buff))
		if err != nil {
			return nil, err
		}
		defer body.Close()

		return ioutil.ReadAll(body)
	}
	return buff, nil
}

// New constructs a new memory backed object store that implements the
// objstore.Store interface.
func New() *Store {
	return &Store{
		fs: afero.NewMemMapFs(),
	}
}
