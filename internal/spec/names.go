package spec

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

var initialisms = map[string]bool{
	"id": true, "url": true, "uri": true, "sku": true, "uuid": true, "api": true, "http": true, "https": true,
	"ttl": true, "json": true, "ip": true, "sql": true, "tls": true, "nats": true, "kv": true, "dns": true,
}

// SplitName splits a YAML name into its words.
func SplitName(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '.' || r == '-' })
}

// GoName converts a YAML name into an exported Go identifier.
func GoName(s string) string {
	var b strings.Builder
	for _, part := range SplitName(s) {
		if initialisms[part] {
			b.WriteString(strings.ToUpper(part))
			continue
		}
		r, size := utf8.DecodeRuneInString(part)
		b.WriteRune(unicode.ToUpper(r))
		b.WriteString(part[size:])
	}
	return b.String()
}

// PackageName converts a YAML name into a Go package name.
func PackageName(s string) string {
	return strings.ToLower(strings.Join(SplitName(s), ""))
}
