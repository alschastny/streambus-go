package streambus

import (
	"strconv"
	"strings"
)

// redisSupportsDeleteModes parses INFO server output and checks if version >= 8.2.
func redisSupportsDeleteModes(info string) bool {
	return redisVersionAtLeast(info, "8.2.0")
}

// redisSupportsIdempotency parses INFO server output and checks if version >= 8.6.
func redisSupportsIdempotency(info string) bool {
	return redisVersionAtLeast(info, "8.6.0")
}

func redisVersionAtLeast(info, minVersion string) bool {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			version := strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
			return compareVersion(version, minVersion) >= 0
		}
	}
	return false
}

// compareVersion compares two semantic version strings.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareVersion(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	for i := 0; i < len(aParts) || i < len(bParts); i++ {
		var aNum, bNum int
		if i < len(aParts) {
			aNum, _ = strconv.Atoi(aParts[i])
		}
		if i < len(bParts) {
			bNum, _ = strconv.Atoi(bParts[i])
		}
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
	}
	return 0
}
