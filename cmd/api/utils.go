package main

func parseDateOrDefault(dateStr, defaultStr string) string {
	if dateStr == "" {
		return defaultStr
	}
	return dateStr
}
