package storage

import (
	"bytes"
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
)

// UploadBytes uploads a byte slice to a specified Google Cloud Storage bucket and path.
// It returns an error if the upload fails.
func UploadBytes(data []byte, bucket string, path string) error {
	// Create a buffer from the byte slice.
	buffer := bytes.NewBuffer(data)

	// Create a background context for the upload operation.
	ctx := context.Background()

	// Create a new Google Cloud Storage client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close() // Ensure the client is closed when the function exits.

	// Set a timeout for the upload operation.
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// Create a writer for the specified bucket and path.
	writer := client.Bucket(bucket).Object(path).NewWriter(ctx)

	// Copy the buffer's contents to the writer.
	if _, err := io.Copy(writer, buffer); err != nil {
		return err
	}

	// Close the writer and check for any errors.
	return writer.Close()
}
