package nanoprotocol

// WildcardMatch reports whether a permission pattern (with '*' wildcards)
// matches a tool name. A single '*' matches any run of characters, including
// the empty string.
func WildcardMatch(pattern, s string) bool {
	pi, si := 0, 0
	star, mark := -1, 0
	for si < len(s) {
		switch {
		case pi < len(pattern) && pattern[pi] == s[si]:
			pi++
			si++
		case pi < len(pattern) && pattern[pi] == '*':
			star = pi
			mark = si
			pi++
		case star != -1:
			pi = star + 1
			mark++
			si = mark
		default:
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// EffectivePermission evaluates an ordered rule list for a tool and returns the
// last matching action. An empty rule set (or no match) defaults to allow, so
// agents are unaffected until an explicit policy is configured.
func EffectivePermission(tool string, rules []PermissionRule) PermissionAction {
	var action PermissionAction
	for _, r := range rules {
		if WildcardMatch(r.Pattern, tool) {
			action = r.Action // last match wins
		}
	}
	if action == "" {
		return PermissionAllow
	}
	return action
}

// PermissionApproval is the structured payload of a ChunkKindApproval chunk and
// of the "_pending_approval" session metadata. Unlike a question, it is a
// dedicated permission prompt: Tool names the gated tool, Summary is a short
// human-readable one-liner for this specific call (e.g. the bash command), and
// Arguments carries the raw tool-call JSON so the portal can render a dedicated
// approval card.
type PermissionApproval struct {
	ID   string `json:"id"`
	Tool string `json:"tool"`
	// TypeID is the active agent definition (mode) the decision is scoped to;
	// empty targets the agent's default mode.
	TypeID    string `json:"type_id,omitempty"`
	Summary   string `json:"summary,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// PermissionDecision is a user's answer to a PermissionApproval.
type PermissionDecision string

const (
	PermissionDecisionAllowOnce   PermissionDecision = "allow_once"
	PermissionDecisionAlwaysAllow PermissionDecision = "always_allow"
	PermissionDecisionDeny        PermissionDecision = "deny"
)

// InboundMessage metadata keys used to answer a PermissionApproval. The answer
// rides the normal message frame; the agent poll loop routes it to the broker
// by id instead of starting a new turn.
const (
	MetadataApprovalID = "approval_id"
	MetadataDecision   = "decision"
)
