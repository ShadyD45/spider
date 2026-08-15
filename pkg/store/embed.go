package store

import (
	"embed"
	"fmt"
	"strings"
)

//go:embed sql/schema.sql sql/queries.sql
var sqlFS embed.FS

func loadSQL(name string) string {
	b, err := sqlFS.ReadFile(name)
	if err != nil {
		panic(fmt.Sprintf("embed %s: %v", name, err))
	}
	return string(b)
}

func parseNamedQueries(src string) map[string]string {
	out := make(map[string]string)
	var name strings.Builder
	var body strings.Builder
	flush := func() {
		n := strings.TrimSpace(name.String())
		q := strings.TrimSpace(body.String())
		if n != "" && q != "" {
			out[n] = q
		}
		name.Reset()
		body.Reset()
	}
	for _, line := range strings.Split(src, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "-- name:") {
			flush()
			name.WriteString(strings.TrimSpace(strings.TrimPrefix(trim, "-- name:")))
			continue
		}
		if strings.HasPrefix(trim, "--") {
			continue
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	flush()
	return out
}

func placeholdersPostgres(query string) string {
	n := 0
	var b strings.Builder
	for _, c := range query {
		if c == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}
