package main

import "testing"

func TestParseSources(t *testing.T) {
	cfgs, err := parseSources("srcA:30,srcB:20, srcC : 5")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfgs) != 3 {
		t.Fatalf("expected 3 sources, got %d", len(cfgs))
	}
	if cfgs[0].name != "srcA" || cfgs[0].priority != 30 {
		t.Errorf("srcA = %+v", cfgs[0])
	}
	if cfgs[2].name != "srcC" || cfgs[2].priority != 5 {
		t.Errorf("srcC = %+v", cfgs[2])
	}
}

func TestParseSourcesErrors(t *testing.T) {
	if _, err := parseSources(""); err == nil {
		t.Fatal("expected error for empty")
	}
	if _, err := parseSources("srcA"); err == nil {
		t.Fatal("expected error for missing priority")
	}
	if _, err := parseSources("a:xyz"); err == nil {
		t.Fatal("expected error for non-numeric priority")
	}
}

func TestParseSourcesEmptyList(t *testing.T) {
	if splitComma("") != nil {
		t.Fatal("expected nil for empty comma list")
	}
}
