package execution

import "testing"

func TestSubmissionPolicyDoesNotChangeMatching(t *testing.T) {
	for _, tc := range []struct {
		mode      OrderMode
		limit     string
		exclusive bool
		want      int64
	}{
		{"", "90", true, 0},
		{"", "99.95", true, 10},
		{"", "99.95", false, 0},
		{ExchangeFirst, "90", true, 10},
		{ExchangeFirst, "90", false, 0},
		{ExchangeFirst, "101", false, 10},
	} {
		i := testIntent(Buy)
		i.Conditions.Limit = intentPrice(tc.limit)
		policy := OrderPolicy{Mode: tc.mode}
		if err := policy.Admit(&i, intentPrice("100"), 10, 0, tc.exclusive); err != nil {
			t.Fatal(err)
		}
		got, err := i.AvailableForSubmission(intentPrice("100"), 10, 0)
		if err != nil || got != tc.want {
			t.Fatalf("%+v: got %d err %v", tc, got, err)
		}
		matched, err := i.Evaluate(intentPrice("100"), 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		if tc.limit != "101" && matched != 0 {
			t.Fatal("venue admission fabricated a local fill")
		}
	}
}

func TestSubmissionPolicyCannotBypassTriggerOrExpiry(t *testing.T) {
	i := testIntent(Buy)
	i.Conditions.Limit, i.Conditions.Stop = intentPrice("110"), intentPrice("105")
	p := OrderPolicy{Mode: ExchangeFirst}
	if err := p.Admit(&i, intentPrice("100"), 10, 0, true); err != nil {
		t.Fatal(err)
	}
	if n, _ := i.AvailableForSubmission(intentPrice("100"), 10, 0); n != 0 {
		t.Fatal("unproven native trigger submitted")
	}
	i.Conditions.Stop = intentPrice("0")
	i.Conditions.ExpiresAtMS = 10
	if err := p.Admit(&i, intentPrice("100"), 10, 0, true); err != nil {
		t.Fatal(err)
	}
	if n, _ := i.AvailableForSubmission(intentPrice("100"), 10, 0); n != 0 {
		t.Fatal("expired order submitted")
	}
	if err := (OrderPolicy{Mode: "typo"}).Validate(); err == nil {
		t.Fatal("unknown mode accepted")
	}
}
