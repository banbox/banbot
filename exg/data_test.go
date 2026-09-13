package exg

import "testing"

func TestIsAllowedExgID(t *testing.T) {
	if !IsAllowedExgID("binance") || IsAllowedExgID("unsupported") {
		t.Fatal("exchange allow-list lookup returned an unexpected result")
	}
}
