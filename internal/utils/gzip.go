package utils

import (
	"bytes"
	"compress/gzip"
	"io"
)

func GzipDecompress(data []byte) ([]byte, error) {
	r := bytes.NewReader(data)

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := gzr.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	decompressed, err := io.ReadAll(gzr)
	if err != nil {
		return nil, err
	}

	return decompressed, nil
}
