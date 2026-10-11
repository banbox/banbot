package statistics

import (
	"reflect"
	"testing"
)

func TestAFMLPluginEntropySourceWordBoundary(t *testing.T) {
	h, pmf, err := PluginEntropy("0101", 1)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, pmf["0"], 2./3)
	afmlNear(t, pmf["1"], 1./3)
	afmlNear(t, h, .9182958340544896)
	h, _, err = PluginEntropy("0101", 2)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, h, .5)
	unicode, _, err := PluginEntropy("甲乙甲乙", 1)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, unicode, .9182958340544896)
}

func TestAFMLLempelZivDictionary(t *testing.T) {
	got, err := LempelZivDictionary("aaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"a", "aa", "aaa"}) {
		t.Fatalf("unexpected novel phrase parsing %v", got)
	}
}

func TestAFMLKontoyiannisOverlapAndBudget(t *testing.T) {
	constant, err := KontoyiannisEntropy("aaaaaa", 3, 1000)
	if err != nil {
		t.Fatal(err)
	}
	// One centered point, overlapping match length3, log2(4)/(3+1)=.5.
	afmlNear(t, constant.Entropy, .5)
	if constant.Points != 1 || constant.Matches[0] != "aaa" {
		t.Fatalf("overlap match mismatch %+v", constant)
	}
	unique, err := KontoyiannisEntropy("abcdef", 3, 1000)
	if err != nil {
		t.Fatal(err)
	}
	afmlNear(t, unique.Entropy, 2)
	increasing, err := KontoyiannisEntropy("aaaaaa", 0, 1000)
	if err != nil || increasing.Points != 3 {
		t.Fatalf("increasing estimator %v %+v", err, increasing)
	}
	if _, err = KontoyiannisEntropy("aaaaaa", 3, 1); err == nil {
		t.Fatal("comparison budget ignored")
	}
	if _, err = KontoyiannisEntropy("abcde", 0, 1000); err == nil {
		t.Fatal("odd increasing-window length accepted")
	}
}
