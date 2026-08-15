package metacache

import (
	"context"
	"time"
)

// Nop implements Cache with no storage (every Get misses).
type Nop struct{}

func (Nop) Get(context.Context, string) ([]byte, bool, error)          { return nil, false, nil }
func (Nop) Set(context.Context, string, []byte, time.Duration) error   { return nil }
func (Nop) Delete(context.Context, string) error                       { return nil }
func (Nop) DeletePrefix(context.Context, string) error                 { return nil }
func (Nop) Close() error                                               { return nil }
