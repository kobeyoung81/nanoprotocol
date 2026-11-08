package nanoprotocol

import (
	"encoding/json"
	"testing"
)

func TestWildcardMatch(t *testing.T) {
	cases := []struct {
		pattern string
		s       string
		want    bool
	}{
		{"read", "read", true},
		{"read", "write", false},
		{"*", "anything", true},
		{"mcp__*", "mcp__github", true},
		{"mcp__*", "read", false},
		{"*_skill", "load_skill", true},
		{"*_skill", "read", false},
		{"*", "", true},
	}
	for _, c := range cases {
		if got := WildcardMatch(c.pattern, c.s); got != c.want {
			t.Errorf("WildcardMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestEffectivePermission(t *testing.T) {
	rules := []PermissionRule{
		{Pattern: "*", Action: PermissionDeny},
		{Pattern: "read", Action: PermissionAllow},
	}
	if got := EffectivePermission("read", rules); got != PermissionAllow {
		t.Errorf("EffectivePermission(read) = %q, want allow", got)
	}
	if got := EffectivePermission("bash", rules); got != PermissionDeny {
		t.Errorf("EffectivePermission(bash) = %q, want deny", got)
	}
	if got := EffectivePermission("bash", nil); got != PermissionAllow {
		t.Errorf("EffectivePermission(bash, nil) = %q, want allow", got)
	}
}

func TestPermissionApprovalRoundTrip(t *testing.T) {
	app := PermissionApproval{
		ID:        "abc",
		Tool:      "bash",
		TypeID:    "build",
		Summary:   "rm -rf ./tmp",
		Arguments: `{"command":"rm -rf ./tmp"}`,
	}
	b, err := json.Marshal(app)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got PermissionApproval
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != app {
		t.Fatalf("round-trip mismatch: %+v != %+v", got, app)
	}
}

func TestPermissionDecisionValues(t *testing.T) {
	for _, d := range []PermissionDecision{
		PermissionDecisionAllowOnce,
		PermissionDecisionAlwaysAllow,
		PermissionDecisionDeny,
	} {
		if d == "" {
			t.Fatalf("empty decision constant")
		}
	}
}
