// KVM Cache-Line Overflow & Clean False-Positive Probe (`velda-obs` -> `velda-gw:8443`).
//
// Generates thousands of unique real TLS JA4 fingerprints by varying TLS ClientHello cipher
// suite subsets over real TCP+TLS handshakes to saturate `bot_detection`'s 512-bit (64B)
// L1 Cache-Line Bitfilter and overflow its L2 CLOCK arena (`max_entries`), while probing
// clean baseline clients before/during/after overflow.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const chromeUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

// 12 standard Go TLS 1.2 cipher suites; non-empty subsets give 2^12 - 1 = 4,095 distinct JA4s.
var cipherPool = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
}

func ciphersForSeed(seed int) []uint16 {
	// Always keep ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 + vary a bitmask across remaining 11 suites
	// so every seed negotiates cleanly with ECDSA server certs AND changes JA4_a / JA4_b!
	out := []uint16{cipherPool[0]}
	mask := (seed % 2047) + 1
	for bit := 0; bit < 11; bit++ {
		if (mask & (1 << bit)) != 0 {
			out = append(out, cipherPool[bit+1])
		}
	}
	return out
}

func probeOnce(target string, ciphers []uint16, tls12 bool, headers map[string]string) (int, int64, error) {
	maxVer := uint16(tls.VersionTLS13)
	if tls12 {
		maxVer = tls.VersionTLS12
	}
	tr := &http.Transport{
		DisableKeepAlives: true,
		DialContext: (&net.Dialer{
			Timeout: 3 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "velda-gw",
			MinVersion:         tls.VersionTLS12,
			MaxVersion:         maxVer,
			CipherSuites:       ciphers,
			NextProtos:         []string{"http/1.1"},
		},
	}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	req, _ := http.NewRequest("GET", "https://"+target+"/", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	ms := time.Since(t0).Milliseconds()
	if err != nil {
		return 0, ms, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	tr.CloseIdleConnections()
	return resp.StatusCode, ms, nil
}

func main() {
	target := flag.String("target", "192.168.122.10:8443", "Velda Edge HTTPS address")
	uniqueBots := flag.Int("unique-bots", 2000, "Distinct bot JA4 identities to flood")
	concurrency := flag.Int("concurrency", 200, "Concurrent worker goroutines")
	sustainSecs := flag.Int("sustain-secs", 0, "Keep hammering L1/L2 in background for N seconds during wrk")
	flag.Parse()

	spoofHeaders := map[string]string{
		"User-Agent":      chromeUA,
		"Accept":          "text/html,application/xhtml+xml",
		"Accept-Language": "en-US,en;q=0.9",
		"Sec-Fetch-Site":  "none",
		"Sec-Fetch-Mode":  "navigate",
		"Sec-Fetch-Dest":  "document",
	}
	cleanHeaders := map[string]string{
		"User-Agent": "velda-healthcheck/1.0",
		"Accept":     "*/*",
	}

	// Reserve `CipherSuites: nil` exclusively for clean innocent clients so L1 saturation never shares bot JA4s.
	botFirstCiphers := ciphersForSeed(1)
	botLastSeed := *uniqueBots - 1
	botLastCiphers := ciphersForSeed(botLastSeed)

	fmt.Printf("[Phase 1] Pre-flood check on clean client & 2-strike bot#1 -> %s\n", *target)
	code, ms, err := probeOnce(*target, nil, false, cleanHeaders)
	fmt.Printf("  clean pre-flood: status=%d rtt=%dms err=%v\n", code, ms, err)
	for s := 0; s < 2; s++ {
		code, ms, err = probeOnce(*target, botFirstCiphers, false, spoofHeaders)
		fmt.Printf("  bot#1 strike#%d: status=%d rtt=%dms err=%v\n", s, code, ms, err)
	}

	fmt.Printf("\n[Phase 2] Flooding %d unique bot JA4s across %d workers (saturating 512-bit L1 + overflowing L2)...\n",
		*uniqueBots, *concurrency)
	var ok200, errCnt, delayedCnt atomic.Int64
	var idx atomic.Int64
	idx.Store(1)
	tStart := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(idx.Add(1))
				if i >= *uniqueBots {
					return
				}
				tls12 := (i >= 2047)
				code, rtt, err := probeOnce(*target, ciphersForSeed(i), tls12, spoofHeaders)
				if err != nil {
					errCnt.Add(1)
					continue
				}
				if code == 200 {
					ok200.Add(1)
				}
				if rtt >= 80 {
					delayedCnt.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	// Give newest bot (`bot#end`) a 2nd strike so L2 slot has strike >= 1 (testable via sneaky clean UA).
	codeLast, msLast, _ := probeOnce(*target, botLastCiphers, botLastSeed >= 2047, spoofHeaders)
	elapsed := time.Since(tStart)
	fmt.Printf("  flood complete in %.2fs: ok200=%d delayed(>=80ms)=%d errors=%d (bot#end strike#1 status=%d rtt=%dms)\n",
		elapsed.Seconds(), ok200.Load(), delayedCnt.Load(), errCnt.Load(), codeLast, msLast)

	fmt.Println("\n[Phase 3] Clean verification UNDER saturated L1 512-bit cache line (200 clean requests)...")
	var cleanOK, cleanBad, cleanSlow atomic.Int64
	for i := 0; i < 200; i++ {
		c, rtt, e := probeOnce(*target, nil, false, cleanHeaders)
		if e == nil && c == 200 {
			cleanOK.Add(1)
			if rtt >= 80 {
				cleanSlow.Add(1)
			}
		} else {
			cleanBad.Add(1)
		}
	}
	fmt.Printf("  clean 200 reqs under saturated L1: 200OK=%d slow(>=80ms)=%d bad_or_403=%d\n",
		cleanOK.Load(), cleanSlow.Load(), cleanBad.Load())

	fmt.Println("\n[Phase 4] L2 eviction vs retention probe (bot#1 oldest vs bot#end recent, sending clean UA):")
	c0, ms0, e0 := probeOnce(*target, botFirstCiphers, false, cleanHeaders)
	cL, msL, eL := probeOnce(*target, botLastCiphers, botLastSeed >= 2047, cleanHeaders)
	fmt.Printf("  bot#1   (oldest, expected CLOCK-evicted when >max_entries): status=%d rtt=%dms err=%v\n", c0, ms0, e0)
	fmt.Printf("  bot#end (recent, expected retained in L2 at strike>=1):     status=%d rtt=%dms err=%v\n", cL, msL, eL)

	if *sustainSecs > 0 {
		fmt.Printf("\n[Phase 5] Sustaining background bot churn for %ds for concurrent wrk/perf stress...\n", *sustainSecs)
		deadline := time.Now().Add(time.Duration(*sustainSecs) * time.Second)
		var swg sync.WaitGroup
		for w := 0; w < 64; w++ {
			idBase := w * 30
			swg.Add(1)
			go func(base int) {
				defer swg.Done()
				k := 0
				for time.Now().Before(deadline) {
					seed := 2 + ((base + k) % 2000)
					k++
					_, _, _ = probeOnce(*target, ciphersForSeed(seed), false, spoofHeaders)
				}
			}(idBase)
		}
		swg.Wait()
	}
}
