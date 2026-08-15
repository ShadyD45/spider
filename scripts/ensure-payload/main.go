// ensure-payload creates or resizes a benchmark payload file (used by compose benchmark scripts).
package main

import (
	"fmt"
	"os"
	"strconv"

	"spider/pkg/benchmark"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: ensure-payload <path> <sizeBytes>\n")
		os.Exit(2)
	}
	size, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid size: %v\n", err)
		os.Exit(1)
	}
	if err := benchmark.EnsurePayloadFile(os.Args[1], size); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
