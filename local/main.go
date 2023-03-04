package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type State struct {
	baseDir string
}

func New(baseDir string) (*State, error) {
	// check if baseDir exists, mkdir it if not:
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		err = os.MkdirAll(baseDir, 0750)
		if err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", baseDir, err)
		}
	}
	s := &State{
		baseDir: baseDir,
	}
	return s, nil
}

func (s State) OpenObjectForWriting(_ context.Context, name string) (io.WriteCloser, error) {
	if s.baseDir == "" {
		return nil, errors.New("basedir not set")
	}
	p := path.Join(s.baseDir, name)
	dir := strings.Split(p, string(filepath.Separator))
	dir = dir[:len(dir)-1]
	dirs := filepath.Join(dir...)
	err := os.MkdirAll(dirs, 0750)
	if err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dirs, err)
	}
	return os.Create(p)
}
func (s State) OpenObjectForReading(_ context.Context, name string) (io.ReadCloser, error) {
	if s.baseDir == "" {
		return nil, errors.New("basedir not set")
	}
	p := path.Join(s.baseDir, name)

	file, err := os.Open(p)
	return file, err
}
func (s State) DeleteObject(_ context.Context, name string) error {
	if s.baseDir == "" {
		return errors.New("basedir not set")
	}
	p := path.Join(s.baseDir, name)
	return os.Remove(p)
}

func (s State) ListObjects(_ context.Context) ([]string, error) {
	if s.baseDir == "" {
		return nil, errors.New("basedir not set")
	}
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, fmt.Errorf("os.ReadDir: %v", err)
	}
	l := make([]string, 0, len(entries))
	for _, entry := range entries {
		l = append(l, entry.Name())
	}
	return l, nil
}
