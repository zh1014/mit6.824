package util

import (
	"fmt"
	"testing"
)

func TestUint32(t *testing.T) {
	i := 1<<63 - 1
	i++
	fmt.Println(i)
}
