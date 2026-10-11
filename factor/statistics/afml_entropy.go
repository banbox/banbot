package statistics

import (
	"errors"
	"math"
	"sort"
)

// PluginEntropy implements AFML snippet18.1, in bits per character. The source
// range(w,len(msg)) intentionally yields len(msg)-w words, excluding the final
// possible word. Rune indexing matches Python Unicode characters. The PMF is
// returned so its fitted alphabet/word probabilities can be persisted.
func PluginEntropy(message string, wordLength int) (float64, map[string]float64, error) {
	runes := []rune(message)
	if wordLength < 1 || len(runes) <= wordLength {
		return 0, nil, errors.New("statistics: plug-in entropy requires more characters than word length")
	}
	pmf := make(map[string]float64)
	for end := wordLength; end < len(runes); end++ {
		pmf[string(runes[end-wordLength:end])]++
	}
	keys := make([]string, 0, len(pmf))
	for key := range pmf {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entropy := 0.
	for _, key := range keys {
		p := pmf[key] / float64(len(runes)-wordLength)
		pmf[key] = p
		entropy -= p * math.Log2(p) / float64(wordLength)
	}
	return entropy, pmf, nil
}

// LempelZivDictionary implements snippet18.2's shortest novel phrase parsing.
// A terminal suffix already in the dictionary is not inserted a second time.
func LempelZivDictionary(message string) ([]string, error) {
	x := []rune(message)
	if len(x) == 0 {
		return nil, errors.New("statistics: LZ dictionary needs a nonempty message")
	}
	dictionary := []string{string(x[:1])}
	seen := map[string]bool{dictionary[0]: true}
	for start := 1; start < len(x); {
		found := false
		for end := start + 1; end <= len(x); end++ {
			phrase := string(x[start:end])
			if !seen[phrase] {
				seen[phrase] = true
				dictionary = append(dictionary, phrase)
				start, found = end, true
				break
			}
		}
		if !found {
			break
		}
	}
	return dictionary, nil
}

type KontoyiannisResult struct {
	Entropy, Redundancy float64
	Points              int
	Matches             []string
}

// KontoyiannisEntropy implements the centered/increasing-window estimator of
// snippets18.3–18.4. Each term is log2(n+1)/(longest match+1), averaged across
// points; match substrings may overlap the decision point, as in the source.
// window=0 selects increasing n=i and requires an even message length; positive
// window is clamped to floor(length/2). maxComparisons bounds actual character
// comparisons. This whole-message diagnostic is not a live causal feature:
// fitting must use only the historical prefix visible at its feature timestamp.
func KontoyiannisEntropy(message string, window int, maxComparisons int64) (KontoyiannisResult, error) {
	x := []rune(message)
	length := len(x)
	if length < 2 || window < 0 || maxComparisons < 1 || (window == 0 && length%2 != 0) {
		return KontoyiannisResult{}, errors.New("statistics: invalid Kontoyiannis window/message/budget")
	}
	first, last := 1, length/2
	if window > 0 {
		window = min(window, length/2)
		first, last = window, length-window
	}
	var result KontoyiannisResult
	comparisons := int64(0)
	for i := first; i <= last; i++ {
		n := window
		if n == 0 {
			n = i
		}
		best := 0
		for j := i - n; j < i; j++ {
			matched := 0
			for matched < n {
				if comparisons >= maxComparisons {
					return KontoyiannisResult{}, errors.New("statistics: Kontoyiannis comparison budget exceeded")
				}
				comparisons++
				if x[j+matched] != x[i+matched] {
					break
				}
				matched++
			}
			best = max(best, matched)
		}
		result.Entropy += math.Log2(float64(n)+1) / float64(best+1)
		result.Matches = append(result.Matches, string(x[i:i+best]))
		result.Points++
	}
	result.Entropy /= float64(result.Points)
	result.Redundancy = 1 - result.Entropy/math.Log2(float64(length))
	return result, nil
}
