package nanoprotocol

import "strings"

// SanitizeName normalizes an arbitrary display name into a safe filesystem /
// POSIX identifier: lowercase, only [a-z0-9_-], runs of invalid characters
// collapsed to a single hyphen, leading/trailing hyphens trimmed, truncated to
// maxLen bytes. It returns the empty string when the result would be empty —
// callers decide their fallback (e.g. an id-based identifier). A capital "A"
// does not become the separator "-".
//
// Used by the runner (Linux username for the agent container) and nanosec
// (agent home folder names), so both stay on the same sanitization rules.
func SanitizeName(name string, maxLen int) string {
	var b strings.Builder
	lastSep := true // leading separators are trimmed
	for _, r := range name {
		if b.Len() >= maxLen {
			break
		}
		var ch byte
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			ch = byte(r)
		case r >= 'A' && r <= 'Z':
			ch = byte(r - 'A' + 'a')
		default:
			ch = '-' // separator
		}
		if ch != '-' {
			if lastSep && b.Len() > 0 && b.Len() < maxLen {
				b.WriteByte('-')
			}
			b.WriteByte(ch)
			lastSep = false
			continue
		}
		lastSep = true
	}
	out := b.String()
	if len(out) > maxLen {
		out = out[:maxLen]
	}
	return out
}