package dev

import "testing"

func TestValidateWebAuthRequiresPasswordOnPublicBind(t *testing.T) {
	if err := validateWebAuth("0.0.0.0", ""); err == nil {
		t.Fatal("expected password requirement for 0.0.0.0")
	}
	if err := validateWebAuth("0.0.0.0", "secret"); err != nil {
		t.Fatal(err)
	}
	if err := validateWebAuth("127.0.0.1", ""); err != nil {
		t.Fatal(err)
	}
}
