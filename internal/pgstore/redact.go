package pgstore

import (
	"net/url"
	"strings"
)

const (
	redactedPassword = "xxxxx"
	// unparsableURL replaces a connection string RedactURL cannot parse: a
	// malformed one can put the password anywhere, so none of it is kept.
	unparsableURL = "<unparsable connection string>"
)

// RedactURL returns a PostgreSQL connection string (URL or libpq
// keyword/value form) with its password replaced, for logs and errors
// (plan ground rule 7). The password is dropped from the userinfo, and every
// parameter value not known to be harmless is replaced. Any other URL (an
// S3 endpoint, say) gets the same treatment.
func RedactURL(raw string) string {
	if raw == "" {
		return ""
	}
	if hasScheme(raw) {
		return redactURLForm(raw)
	}
	return redactKeywordForm(raw)
}

// hasScheme reports whether raw starts with "scheme://", which is how pgx
// tells a URL from the keyword/value form. A "://" later on is just part of
// a keyword value.
func hasScheme(raw string) bool {
	scheme, _, ok := strings.Cut(raw, "://")
	if !ok || scheme == "" {
		return false
	}
	for i := 0; i < len(scheme); i++ {
		c := scheme[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (i == 0 || ((c < '0' || c > '9') && c != '+' && c != '-' && c != '.')) {
			return false
		}
	}
	return true
}

func redactURLForm(raw string) string {
	u, err := url.Parse(raw)
	// A fragment or opaque part means the URL is malformed in a way that
	// can hide a password where neither the userinfo nor the query sees it
	// ("postgres://u@#/db?password=...").
	if err != nil || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" || strings.Contains(raw, "#") {
		return unparsableURL
	}
	if u.User != nil {
		if _, ok := u.User.Password(); ok {
			u.User = url.UserPassword(u.User.Username(), redactedPassword)
		}
	}
	if u.RawQuery != "" {
		// url.Query drops pairs it cannot parse, and a dropped pair could
		// be the password, so a query that fails to parse is unparsable.
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return unparsableURL
		}
		for k, vs := range q {
			if !safeParam(k) {
				for i := range vs {
					vs[i] = redactedPassword
				}
			}
		}
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// safeParam lists the connection parameters whose values are kept. Every
// other value is redacted: besides password there is sslpassword, and a
// malformed URL can turn the password into the value of a garbage key
// ("postgres://u@?/db?password=..." parses as key "/db?password").
func safeParam(key string) bool {
	switch key {
	case "host", "hostaddr", "port", "dbname", "user",
		"sslmode", "sslrootcert", "sslcert", "sslkey", "sslsni", "sslnegotiation",
		"application_name", "connect_timeout", "target_session_attrs":
		return true
	}
	// pgxpool's own settings (pool_max_conns and friends).
	return strings.HasPrefix(key, "pool_")
}

// redactKeywordForm tokenizes libpq's "key=value key='quoted value'" form
// (backslash escapes, optional spaces around '=') and rewrites every value
// safeParam does not keep.
func redactKeywordForm(raw string) string {
	var out []string
	i, n := 0, len(raw)
	isSpace := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' }
	for {
		for i < n && isSpace(raw[i]) {
			i++
		}
		if i == n {
			break
		}
		keyStart := i
		for i < n && raw[i] != '=' && !isSpace(raw[i]) {
			i++
		}
		key := raw[keyStart:i]
		for i < n && isSpace(raw[i]) {
			i++
		}
		if key == "" || i == n || raw[i] != '=' {
			return unparsableURL
		}
		i++
		for i < n && isSpace(raw[i]) {
			i++
		}
		valStart := i
		if i < n && raw[i] == '\'' {
			i++
			for {
				if i >= n {
					return unparsableURL
				}
				if raw[i] == '\\' {
					i += 2
					continue
				}
				if raw[i] == '\'' {
					i++
					break
				}
				i++
			}
		} else {
			for i < n && !isSpace(raw[i]) {
				if raw[i] == '\\' {
					i++
				}
				i++
			}
		}
		if i > n {
			return unparsableURL
		}
		val := raw[valStart:i]
		if !safeParam(key) {
			val = redactedPassword
		}
		out = append(out, key+"="+val)
	}
	return strings.Join(out, " ")
}
