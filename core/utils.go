package core

import (
	"fmt"
	"slices"
	"strings"
)

/*
GroupByPairQuotes
format `[key]:pairs...` as below
【key】
Quote: Base1 Base2 ...
*/
func GroupByPairQuotes(items map[string][]string, doSort bool) string {
	res := make(map[string]map[string][]string)
	for key, arr := range items {
		if doSort {
			slices.Sort(arr)
		}
		quoteMap := make(map[string][]string)
		for _, pair := range arr {
			baseCode, quoteCode, _, _ := SplitSymbol(pair)
			baseList, _ := quoteMap[quoteCode]
			quoteMap[quoteCode] = append(baseList, baseCode)
		}
		for quote, baseList := range quoteMap {
			if doSort {
				slices.Sort(baseList)
			}
			quoteMap[quote] = baseList
		}
		res[key] = quoteMap
	}
	var b strings.Builder
	for key, quoteMap := range res {
		b.WriteString(fmt.Sprintf("【%s】\n", key))
		for quoteCode, arr := range quoteMap {
			baseStr := strings.Join(arr, " ")
			b.WriteString(fmt.Sprintf("%s(%d): %s\n", quoteCode, len(arr), baseStr))
		}
	}
	return b.String()
}

/*
SplitSymbol
return Base，Quote，Settle，Identifier
*/
func SplitSymbol(pair string) (string, string, string, string) {
	parts := splitSymbolParts(pair)
	return parts[0], parts[1], parts[2], parts[3]
}

func splitSymbolParts(pair string) [4]string {
	var parts [4]string
	start := 0
	for field := 0; field < len(parts); field++ {
		rel := strings.IndexAny(pair[start:], "/:-")
		if rel < 0 {
			parts[field] = pair[start:]
			break
		}
		parts[field] = pair[start : start+rel]
		start += rel + 1
	}
	return parts
}
