package utils

import "github.com/go-gota/gota/dataframe"

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func GetStr(col string, rowIdx int, df *dataframe.DataFrame) string {

	if df == nil {
		return ""
	}

	if containsString(df.Names(), col) {
		return df.Col(col).Elem(rowIdx).String()
	}
	return ""
}

func GetInt(col string, rowIdx int, df *dataframe.DataFrame) int {
	if df == nil {
		return 0
	}
	if idx := df.Names(); containsString(idx, col) {
		val, err := df.Col(col).Elem(rowIdx).Int()
		if err != nil {
			return 0
		}
		return val
	}
	return 0
}
