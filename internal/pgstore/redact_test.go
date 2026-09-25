package pgstore_test

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/stretchr/testify/require"
)

func TestRedactURL(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]string{
		"": "",
		"postgres://jetstream:s3cret@127.0.0.1:15432/jetstream?sslmode=disable": "postgres://jetstream:xxxxx@127.0.0.1:15432/jetstream?sslmode=disable",
		"postgresql://u:p%40ss@db.example.com/x":                                "postgresql://u:xxxxx@db.example.com/x",
		"postgres://u@h/db":                                                     "postgres://u@h/db",
		"postgres://h/db?password=s3cret&sslmode=verify-full":                   "postgres://h/db?password=xxxxx&sslmode=verify-full",
		"postgres://u:s3cret@h1:5432,h2:5433/db":                                "postgres://u:xxxxx@h1:5432,h2:5433/db",
		"host=h user=u password=s3cret dbname=d":                                "host=h user=u password=xxxxx dbname=d",
		"host=h password = 's3 cret\\' x' sslmode=require":                      "host=h password=xxxxx sslmode=require",
		"password=s3\\ cret host=h":                                             "password=xxxxx host=h",
		"host=h password='unterminated s3cret":                                  "<unparsable connection string>",
		"host=h s3cret":                                                         "<unparsable connection string>",
		"password=s3cret\\":                                                     "<unparsable connection string>",
		"postgres://u:s3cret@h/db?password=%zz":                                 "<unparsable connection string>",
		"postgres://u:s3cret@[::1/db":                                           "<unparsable connection string>",
		"http://access:s3cret@127.0.0.1:18333":                                  "http://access:xxxxx@127.0.0.1:18333",
		"https://s3.us-east-1.amazonaws.com":                                    "https://s3.us-east-1.amazonaws.com",
	} {
		require.Equal(t, want, pgstore.RedactURL(in), "input %q", in)
	}
}

func TestConfigRedactsURL(t *testing.T) {
	t.Parallel()

	cfg := pgstore.Config{URL: "postgres://u:s3cret@h/db", MaxConns: 4}
	var sb strings.Builder
	logger := slog.New(slog.NewJSONHandler(&sb, nil))
	logger.Info("cfg", "pg", cfg)
	for _, s := range []string{fmt.Sprint(cfg), fmt.Sprintf("%v %+v %#v %s", cfg, cfg, cfg, cfg), sb.String()} {
		require.NotContains(t, s, "s3cret")
		require.Contains(t, s, "xxxxx")
	}
}

// FuzzRedactURL checks the one property that matters: a password planted in
// any connection string form never survives redaction.
func FuzzRedactURL(f *testing.F) {
	f.Add("s3cret", "db.example.com", "sslmode=disable")
	f.Add("p@ss:w/rd", "h1:5432,h2:5433", "")
	f.Add("it's", "h", "a=b")
	f.Fuzz(func(t *testing.T, password, host, extra string) {
		if len(password) < 4 {
			t.Skip()
		}
		escaped := escapeURL(password)
		quoted := "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(password) + "'"
		for _, form := range []func(userinfoPW, queryPW, keywordPW string) string{
			func(pw, _, _ string) string { return "postgres://jetstream:" + pw + "@" + host + "/db?" + extra },
			func(_, pw, _ string) string { return "postgres://jetstream@" + host + "/db?password=" + pw },
			func(_, _, pw string) string { return "host=" + host + " user=jetstream password=" + pw + " dbname=db" },
			func(_, _, pw string) string { return "password = " + pw + " host=" + host },
		} {
			// Skip passwords that show up by coincidence in the rest of the
			// string, as given or as redaction re-encodes it (a password of
			// "5432" matches the port).
			rest := form("", "", "''") + pgstore.RedactURL(form("q", "q", "q"))
			if strings.Contains(rest, password) || strings.Contains(rest, escaped) {
				continue
			}
			in := form(escaped, escaped, quoted)
			out := pgstore.RedactURL(in)
			require.NotContains(t, out, password, "input %q", in)
			require.NotContains(t, out, escaped, "input %q", in)
		}
	})
}

// escapeURL percent-encodes everything but alphanumerics, which is valid in
// both the userinfo and a query value.
func escapeURL(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}
