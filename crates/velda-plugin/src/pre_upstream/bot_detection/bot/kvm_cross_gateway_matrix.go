// Cross-Gateway Deep Anti-Bot & Zero-False-Positive Matrix (`velda-obs` -> `velda-gw:8443`).
//
// Evaluates the exact same 8 live TLS/HTTP vectors across Velda Edge (`bot_detection` ONLY,
// no `ja4_filter`), Nginx, HAProxy, Envoy, Apache APISIX, and Kong:
//
//	Legitimate clients (MUST pass 200 OK at 0ms delay / zero false positives):
//	  C1: Honest service / healthcheck (`velda-healthcheck/1.0`)
//	  C2: Honest Firefox 132 + Sec-Fetch-* + Accept-Language
//	  C3: Honest Safari 17.5 + Sec-Fetch-* + Accept-Language
//	Adversarial bots (increasing sophistication):
//	  B1: Honest script UA (`python-requests/2.31.0`)
//	  B2: Chrome UA spoof missing Sec-Fetch-* & Accept-Language
//	  B3: Full-header Chrome 131 bot over Go TLS + HTTP/1.1 (spoofs Sec-Fetch-* & Accept-Language)
//	  B4: Full-header Chrome 131 bot over Go HTTP/2 (`crypto/tls` + `h2`)
//	  B5: Sneaky alternating bot (3x B3 spoof then switches to clean UA `velda-healthcheck/1.0`)
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const (
	chromeUA  = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	firefoxUA = "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0"
	safariUA  = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
)

func sendReq(target, path string, h2 bool, ciphers []uint16, headers map[string]string) (int, int64, string) {
	alpn := []string{"http/1.1"}
	if h2 {
		alpn = []string{"h2"}
	}
	tr := &http.Transport{
		DisableKeepAlives: true,
		ForceAttemptHTTP2: h2,
		DialContext: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "velda-gw",
			NextProtos:         alpn,
			CipherSuites:       ciphers,
		},
	}
	client := &http.Client{Transport: tr, Timeout: 6 * time.Second}
	req, _ := http.NewRequest("GET", "https://"+target+path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	ms := time.Since(t0).Milliseconds()
	if err != nil {
		return 0, ms, fmt.Sprintf("ERR(%v)", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	tr.CloseIdleConnections()
	return resp.StatusCode, ms, resp.Proto
}

func runVector(label, target string, h2 bool, ciphers []uint16, headers map[string]string) (int, int64) {
	code, ms, proto := sendReq(target, "/ping", h2, ciphers, headers)
	fmt.Printf("  %-44s -> status=%d delay/rtt=%4dms proto=%s\n", label, code, ms, proto)
	return code, ms
}

func main() {
	name := flag.String("gateway", "velda", "Gateway name under test")
	target := flag.String("target", "192.168.122.10:8443", "Gateway HTTPS address")
	testH2 := flag.Bool("h2", false, "Also run B4 over HTTP/2")
	flag.Parse()

	// Distinct cipher slice per vector so each scenario has an isolated JA4 inside the 120s window.
	cC1 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}
	cC2 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}
	cC3 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256}
	cB1 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}
	cB2 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256}
	cB3 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256}
	cB4 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}
	cB5 := []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384}

	fmt.Printf("=== Cross-Gateway Matrix [%s] (%s) ===\n", *name, *target)

	// Group 1: Legitimate clients (ZERO false positives expected)
	runVector("C1 Honest API (velda-healthcheck/1.0)", *target, false, cC1, map[string]string{
		"User-Agent": "velda-healthcheck/1.0",
		"Accept":     "*/*",
	})
	runVector("C2 Honest Firefox 132 + Sec-Fetch-*", *target, false, cC2, map[string]string{
		"User-Agent":      firefoxUA,
		"Accept":          "text/html,application/xhtml+xml",
		"Accept-Language": "en-US,en;q=0.9",
		"Sec-Fetch-Site":  "none",
		"Sec-Fetch-Mode":  "navigate",
		"Sec-Fetch-Dest":  "document",
	})
	runVector("C3 Honest Safari 17.5 + Sec-Fetch-*", *target, false, cC3, map[string]string{
		"User-Agent":      safariUA,
		"Accept":          "text/html,application/xhtml+xml",
		"Accept-Language": "en-US,en;q=0.9",
		"Sec-Fetch-Site":  "none",
		"Sec-Fetch-Mode":  "navigate",
		"Sec-Fetch-Dest":  "document",
	})

	// Group 2: Escalating bots
	runVector("B1 Honest script UA (python-requests)", *target, false, cB1, map[string]string{
		"User-Agent": "python-requests/2.31.0",
		"Accept":     "*/*",
	})
	runVector("B2 Chrome 131 UA, missing Sec-Fetch-*", *target, false, cB2, map[string]string{
		"User-Agent": chromeUA,
		"Accept":     "*/*",
	})
	runVector("B3 Full-header Chrome 131 spoof (H1)", *target, false, cB3, map[string]string{
		"User-Agent":      chromeUA,
		"Accept":          "text/html,application/xhtml+xml",
		"Accept-Language": "en-US,en;q=0.9",
		"Sec-Fetch-Site":  "none",
		"Sec-Fetch-Mode":  "navigate",
		"Sec-Fetch-Dest":  "document",
	})
	if *testH2 {
		runVector("B4 Full-header Chrome 131 spoof (H2)", *target, true, cB4, map[string]string{
			"User-Agent":      chromeUA,
			"Accept":          "text/html,application/xhtml+xml",
			"Accept-Language": "en-US,en;q=0.9",
			"Sec-Fetch-Site":  "none",
			"Sec-Fetch-Mode":  "navigate",
			"Sec-Fetch-Dest":  "document",
		})
	}

	// B5: 2 strikes spoofing Chrome headers then switch back to honest API UA on same (ip, ja4)
	fullSpoof := map[string]string{
		"User-Agent":      chromeUA,
		"Accept":          "text/html,application/xhtml+xml",
		"Accept-Language": "en-US,en;q=0.9",
		"Sec-Fetch-Site":  "none",
		"Sec-Fetch-Mode":  "navigate",
		"Sec-Fetch-Dest":  "document",
	}
	runVector("B5.1 Spoof strike#0", *target, false, cB5, fullSpoof)
	runVector("B5.2 Spoof strike#1", *target, false, cB5, fullSpoof)
	runVector("B5.3 Sneaky switch -> honest UA", *target, false, cB5, map[string]string{
		"User-Agent": "velda-healthcheck/1.0",
		"Accept":     "*/*",
	})
}
