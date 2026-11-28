// Direct KVM HTTP/2 request-spoofing & sneaky-evasion probes for `bot_detection`.
//
// Target defaults to `192.168.122.10:8443` (`velda-gw` KVM).
// Uses distinct `CipherSuites` lists so honest Go worker traffic and Go H2 Chrome-spoofing
// probes get distinct `(client_ip, ja4)` slots even from a single observer VM (`velda-obs`).
package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

const chromeUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

func makeH2Client(ciphers []uint16) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	tr := &http.Transport{
		DialContext:       dialer.DialContext,
		ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "velda-gw",
			NextProtos:         []string{"h2"},
			CipherSuites:       ciphers,
		},
	}
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func doReq(client *http.Client, targetAddr string, headers map[string]string) (int, int64, error) {
	req, err := http.NewRequest("GET", "https://"+targetAddr+"/ping", nil)
	if err != nil {
		return 0, 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	rtt := time.Since(t0).Milliseconds()
	if err != nil {
		return 0, rtt, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, rtt, nil
}

func main() {
	addr := "192.168.122.10:8443"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	honestClient := makeH2Client([]uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256})
	botClient := makeH2Client([]uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384})

	fmt.Printf("=== [Go H2 KVM Probe 1] Honest Go service -> %s (expect 200 OK, <80ms) ===\n", addr)
	for i := 0; i < 3; i++ {
		code, rtt, err := doReq(honestClient, addr, map[string]string{
			"User-Agent": "billing-worker/2.1",
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("  req#%d status=%d rtt=%dms\n", i+1, code, rtt)
		if code != 200 || rtt >= 80 {
			fmt.Fprintf(os.Stderr, "FALSE POSITIVE on honest Go client: status=%d rtt=%dms\n", code, rtt)
			os.Exit(1)
		}
	}

	fmt.Println("\n=== [Go H2 KVM Probe 2] Go bot spoofing Chrome UA + Sec-Fetch-* -> expect H2 wire+TLS tarpit ===")
	spoofHeaders := map[string]string{
		"User-Agent":      chromeUA,
		"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language": "en-US,en;q=0.9",
		"Sec-Fetch-Site":  "same-origin",
		"Sec-Fetch-Mode":  "navigate",
		"Sec-Fetch-Dest":  "document",
	}
	var rtts []int64
	lastCode := 0
	for i := 0; i < 5; i++ {
		if i == 3 {
			fmt.Println("  --> Sneaky bot switches to honest UA after strike#2...")
			code, rtt, err := doReq(botClient, addr, map[string]string{
				"User-Agent": "billing-worker/2.1",
			})
			if err != nil {
				panic(err)
			}
			fmt.Printf("  sneaky_clean status=%d rtt=%dms\n", code, rtt)
			if rtt < 80 {
				fmt.Fprintf(os.Stderr, "Expected L1+L2 memory to throttle sneaky clean request, got %dms\n", rtt)
				os.Exit(1)
			}
		}
		code, rtt, err := doReq(botClient, addr, spoofHeaders)
		if err != nil {
			panic(err)
		}
		fmt.Printf("  strike#%d status=%d rtt=%dms\n", i, code, rtt)
		rtts = append(rtts, rtt)
		lastCode = code
	}

	if rtts[0] < 80 || (lastCode != 403 && rtts[len(rtts)-1] < 80) {
		fmt.Fprintf(os.Stderr, "Expected progressive tarpit, rtts=%v lastCode=%d\n", rtts, lastCode)
		os.Exit(1)
	}

	fmt.Println("\n=== [Go H2 KVM Probe 3] Honest Go client still unaffected ===")
	code, rtt, err := doReq(honestClient, addr, map[string]string{
		"User-Agent": "billing-worker/2.1",
	})
	if err != nil || code != 200 || rtt >= 80 {
		fmt.Fprintf(os.Stderr, "Isolation failed: err=%v code=%d rtt=%dms\n", err, code, rtt)
		os.Exit(1)
	}
	fmt.Printf("  honest status=%d rtt=%dms\n", code, rtt)
}
