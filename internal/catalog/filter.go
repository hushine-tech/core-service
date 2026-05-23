package catalog

import "strings"

func filterSymbols(all []string, queryUpper string, limit int) []string {
	if limit <= 0 {
		limit = 80
	}
	var out []string
	for _, s := range all {
		su := strings.ToUpper(strings.TrimSpace(s))
		if su == "" {
			continue
		}
		if queryUpper != "" && !strings.Contains(su, queryUpper) {
			continue
		}
		out = append(out, su)
		if len(out) >= limit {
			break
		}
	}
	return out
}
