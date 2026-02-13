package main

import "time"

func parseDateOrDefault(dateStr, defaultStr string) string {
	if dateStr == "" {
		return defaultStr
	}
	return dateStr
}

func parseTime(dateStr string) (time.Time, error) {
	return time.Parse("2006-01-02", dateStr)
}
