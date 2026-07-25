package redisbus

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	)
}
