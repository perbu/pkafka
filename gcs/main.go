package gcs

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"os"
)

type State struct {
	client     *storage.Client
	bucketName string
}

// NewClient creates a new GCS client. On top of the usual GCS client, it also
// specifies the project ID and bucket name.
func NewClient(ctx context.Context, bucketName string) (*State, error) {
	credFile, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
	if !ok {
		return nil, fmt.Errorf("env/GOOGLE_APPLICATION_CREDENTIALS not set")
	}
	if _, err := os.Stat(credFile); err != nil {
		return nil, fmt.Errorf("couldn't find credentials file '%s': %w", credFile, err)
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	s := &State{
		client:     client,
		bucketName: bucketName,
	}
	return s, nil
}

func (s *State) OpenObjectForWriting(ctx context.Context, name string) (io.WriteCloser, error) {
	wc := s.client.Bucket(s.bucketName).Object(name).NewWriter(ctx)
	return wc, nil
}
func (s *State) OpenObjectForReading(ctx context.Context, name string) (io.ReadCloser, error) {
	rc, err := s.client.Bucket(s.bucketName).Object(name).NewReader(ctx)
	return rc, err
}
func (s *State) DeleteObject(ctx context.Context, name string) error {
	err := s.client.Bucket(s.bucketName).Object(name).Delete(ctx)
	return err
}

func (s *State) ListObjects(ctx context.Context) ([]string, error) {
	var err error
	query := &storage.Query{}
	iterator := s.client.Bucket(s.bucketName).Objects(ctx, query)
	contents := make([]string, 0)
	for {
		var oa *storage.ObjectAttrs
		oa, err = iterator.Next() // err is from the outer scope.
		if err != nil {
			break
		}
		contents = append(contents, oa.Name)
	}
	return contents, err
}
