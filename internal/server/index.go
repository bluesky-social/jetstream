package server

import (
	"fmt"
	"html"
	"net/http"

	"github.com/bluesky-social/jetstream/internal/version"
)

const (
	docsLink = "https://github.com/bluesky-social/jetstream-v2"
)

var (
	rootHTML = fmt.Sprintf(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>jetstream</title>
<style>
  :root {
    color-scheme: light dark;
    --fg: #1a1a1a;
    --muted: #666;
    --bg: #fafafa;
    --border: #ddd;
    --accent: #2860c8;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --fg: #eee;
      --muted: #aaa;
      --bg: #181818;
      --border: #333;
      --accent: #6aa3ff;
    }
  }
  body {
    margin: 0;
    padding: 2rem 1rem;
    color: var(--fg);
    background: var(--bg);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    line-height: 1.5;
  }
  main {
    max-width: 72rem;
    margin: 0 auto;
  }
  h1 {
    margin: 0 0 0.25rem 0;
    font-size: 1.75rem;
  }
  p {
    margin: 0 0 0.6rem 0;
  }
  a {
    color: var(--accent);
  }
  .meta {
    color: var(--muted);
  }
  .links {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin: 1rem 0 1.5rem 0;
  }
  .jet {
    margin: 0;
    padding-top: 1rem;
    border-top: 1px solid var(--border);
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.58rem;
    line-height: 1.08;
    white-space: pre;
  }
</style>
</head>
<body>
<main>
  <h1>Welcome to Jetstream</h1>
  <p class="meta">Version: %s</p>
  <nav class="links" aria-label="jetstream links">
    <a href="/status">Status</a>
    <a href="%s" target="_blank">Source</a>
  </nav>
  <pre class="jet" aria-label="jetstream jet">%s</pre>
</main>
</body>
</html>
`,
		html.EscapeString(version.Get().Version),
		html.EscapeString(docsLink),
		html.EscapeString(asciiArt),
	)
)

// handleRoot returns the public index page.
func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(rootHTML))
}

const asciiArt = `
                                                 ▂▃                                                 
                                                ▂▁▇▄                                                
                                               ▂▁ ▇█▃                                               
                                              ▁▃  ███▁                                              
                                              ▂▁▁▁███▅                                              
                                              ▂▃▆████▇                                              
                                              ▃▄▆▇████▁                                             
                                              ▂▃██████▁                                             
                                              ▂▃██████▂                                             
                                              ▂▂▅▇████▂                                             
                                              ▂ ▁▁████▂                                             
                                             ▁▂  ▁████▂                                             
                                             ▂▂  ▁▇███▅                                             
                                            ▁▂▂   ▇████▁                                            
                                            ▂▁▂  ▁█████▇▁                                           
                                          ▁▂▁ ▂  ▁██████▇▂▁                                         
                                      ▁▁▁▁▁▂▁ ▂  ▁█████████▆▄▂                                      
                                  ▁▂▁▂▁▁▁▁▁▁  ▂ ▁ ████████████▇▅▃▁                                  
                              ▁▁▁▁▁▁▁▁▁▁ ▁    ▂▁  ████████████████▇▅▃▁                              
                           ▁▁▁▁▁▁▁▁▁          ▂   ████████████████████▆▄▂▁                          
                       ▁▁▂▁▁▁▁▁▁ ▁      ▁     ▂  ▁████████████████████████▆▄▂                       
                   ▁▃▁▂▁▁▁▁▁        ▁▁        ▂  ▂███████████████████████████▇▅▃▁                   
                  ▄█▄ ▁▁       ▁▁▁▁▁          ▂  ▁███████████████████████████████▅▁                 
                  ▆█▄▁▁▁▁▁▁▁▁▁▁▁   ▁▂▁▁▁▁▁▁▁▂ ▂  ▁███████████████████████████████▅▂                 
                  ▆▆▄▂▁▁▂▁▂▂▁▂▁▂▂▁▁▂▂▁▁▁▁▂▂▁▂▁▂  ▁████████▇▇▆▆▅▅▄▄▄▄▄▄▄▄▄▅▅▅▅▅▆▆▆▅▂                 
                                             ▁▂  ▁████▄▁                                            
                                              ▂ ▁▁████▃                                             
                                              ▂ ▁▁████▃                                             
                                              ▂  ▂████▂                                             
                                              ▃  ▃████▂                                             
                                              ▂  ▃████                                              
                                              ▂▁ ▄███▇                                              
                                           ▁▁▂▂▂ ▄████▇▅▂▁                                          
                                       ▁▂▁▁▁  ▁▂ ▄████████▆▄▂                                       
                                    ▂▁▁▁     ▁ ▄▄▅████████████▅▃▁                                   
                                    ▄      ▁  ▁▄▄▆██▅███████████▃                                   
                                    ▄▁▁▁▁▁▂▂▁▁▁▁▃▆█▆▁▂▂▃▄▅▆▇▇██▆▃                                   
`
