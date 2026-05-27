package server

import (
	"fmt"
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/version"
)

const (
	docsLink = "https://github.com/bluesky-social/jetstream-v2"
)

var (
	rootStr = fmt.Sprintf(
		"Welcome to Jetstream\nVersion: %s\nDocumentation: %s\n\n%s\n",
		version.Get().Version,
		docsLink,
		asciiArt,
	)
)

// handleRoot returns a small JSON identity payload. It exists primarily to
// give us something real to instrument while the substantive endpoints are
// being designed.
func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(rootStr))
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
                  ▁▂   ▁▁▂▁▁▁▁▁▁ ▁      ▁     ▂  ▁████████████████████████▆▄▂                       
                  ▃▄▁▁▂▁▁▁▁▁        ▁▁        ▂  ▂███████████████████████████▇▅▃▁▅                  
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
