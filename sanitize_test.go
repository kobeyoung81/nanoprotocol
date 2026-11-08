package nanoprotocol

import "testing"

func TestSanitizeName(t *testing.T) {
	cases := []struct {
		name   string
		maxLen int
		want   string
	}{
		{"My Agent", 64, "my-agent"},
		{"  Kobe Young  !", 64, "kobe-young"},
		{"my_agent", 64, "my_agent"},
		{"---foo---", 64, "foo"},
		{"A", 64, "a"},
		{"a", 64, "a"},
		{"aBc-1", 64, "abc-1"},
		{"", 64, ""},
		{"汉字名字", 64, ""},
		{"my-very-long-name-that-exceeds-folder-limits-completely", 16, "my-very-long-nam"},
		{"--double--", 64, "double"},
		{"a--b", 64, "a-b"},
	}
	for _, c := range cases {
		got := SanitizeName(c.name, c.maxLen)
		if got != c.want {
			t.Errorf("SanitizeName(%q, %d) = %q, want %q", c.name, c.maxLen, got, c.want)
		}
	}
}