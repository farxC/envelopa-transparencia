package main

import "fmt"

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
