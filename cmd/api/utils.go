package main

import (
	"fmt"
	"strconv"
	"strings"
)

func parseDateOrDefault(dateStr, defaultStr string) string {
	if dateStr == "" {
		return defaultStr
	}
	return dateStr
}

func parseIntOrDefault(intStr string, defaultVal int) int {
	if intStr == "" {
		return defaultVal
	}
	var val int
	_, err := fmt.Sscanf(intStr, "%d", &val)
	if err != nil {
		return defaultVal
	}
	return val
}

func parseCSVToInts(csv string) []int {
	if csv == "" {
		return []int{}
	}

	parts := strings.Split(csv, ",")
	result := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if num, err := strconv.Atoi(part); err == nil {
			result = append(result, num)
		}
	}

	return result
}
