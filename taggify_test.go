package main

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	header = `# INFLUXDB EXPORT: 1677-09-21T00:32:15+00:19 - 2262-04-12T00:47:16+01:00
# DDL
CREATE DATABASE test WITH NAME test
# DML
# CONTEXT-DATABASE:test
# CONTEXT-RETENTION-POLICY:autogen
# writing tsm data`

	footer = `# writing wal data`
)

func TestTaggify(t *testing.T) {
	for _, tc := range []struct {
		data   string
		result map[string]bool
	}{
		{
			`test,id=foo string="foo" 1511629912071663075
test,id=foo string="foo" 1511629912071663075
test,id=foo idd="bar" 1511629912071663075
test,id=foo int=42 1511629912071663075
test,id=foo idd="bar" 1511629912071663076
test,id=foo int=43 1511629912071663076
test,id=foo,idd=already string="foo" 1511629912071663075`,
			map[string]bool{
				`test,id=foo,idd=bar string="foo" 1511629912071663075`:     true,
				`test,id=foo,idd=bar int=42 1511629912071663075`:           true,
				`test,id=foo,idd=bar int=43 1511629912071663076`:           true,
				`test,id=foo,idd=already string="foo" 1511629912071663075`: true,
			},
		},
	} {
		a := assert.New(t)
		buf := &bytes.Buffer{}

		a.NoError(taggify(strings.NewReader(strings.Join([]string{header, tc.data, footer}, string('\n'))), buf, "idd", "non-existant"))

		out := buf.String()
		if !a.True(len(out) > len(header)+len(footer), "length of output") {
			t.Fatal(out)
		}
		if !a.Equal(header, out[:len(header)], "header") {
			t.Fatal(out[:len(header)])
		}
		if !a.Equal(footer, out[len(out)-len(footer):], "footer") {
			t.Fatal(out[len(out)-len(footer):])
		}

		sc := bufio.NewScanner(strings.NewReader(out[len(header)+1 : len(out)-len(footer)-1]))
		seen := make(map[string]int, len(tc.result))
		for sc.Scan() {
			a.Contains(tc.result, sc.Text(), "Unexpected line: %s", sc.Text())
			seen[sc.Text()]++
		}
		a.NoError(sc.Err())
		for line, i := range seen {
			a.Equalf(1, i, "Times '%s' is outputed", line)
		}
	}
}
