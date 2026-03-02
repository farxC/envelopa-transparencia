package utils

import (
	"strconv"
	"strings"
	"time"
)

func ParseDate(dateStr string) time.Time {
	if dateStr == "" {
		return time.Time{}
	}
	// Try dd/mm/yyyy format first
	t, err := time.Parse("02/01/2006", dateStr)
	if err == nil {
		return t
	}
	// Fallback to yyyy-mm-dd just in case
	t, err = time.Parse("2006-01-02", dateStr)
	if err == nil {
		return t
	}
	return time.Time{}
}

func ParseFloat(valStr string) float64 {
	if valStr == "" {
		return 0.0
	}
	// Remove thousands separator (.) and replace decimal separator (,) with (.)
	cleanStr := strings.ReplaceAll(valStr, ".", "")
	cleanStr = strings.ReplaceAll(cleanStr, ",", ".")
	val, err := strconv.ParseFloat(cleanStr, 64)
	if err != nil {
		return 0.0
	}
	return val
}

func ParseInt64(valStr string) int64 {
	if valStr == "" {
		return 0
	}
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func ParseInt16(val int) int16 {
	return int16(val)
}

func ParseBool(valStr string) bool {
	return strings.EqualFold(valStr, "Sim") || strings.EqualFold(valStr, "Yes") || valStr == "1"
}
