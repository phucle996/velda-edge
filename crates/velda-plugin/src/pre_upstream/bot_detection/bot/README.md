# Direct KVM `bot_detection` Request Spoofing & Flood Scripts

Lightweight request-spoofing scripts designed to hit **`velda-gw` (`192.168.122.10:8443`)** directly on KVM (`bot_detection` only, zero `ja4_filter`).

## Files
- [`probe_bots.py`](probe_bots.py): Python HTTPS/1.1 probes (honest API + `Firefox/132.0` zero-FP baselines vs. Chrome-spoofing script triggering progressive tarpit).
- [`probe_bots.go`](probe_bots.go): Go HTTP/2 probes (honest Go service baseline vs. Go `crypto/tls` + `h2` Chrome spoof + mid-window honest-UA evasion check).
- [`kvm_cross_gateway_matrix.go`](kvm_cross_gateway_matrix.go): Multi-gateway (`Velda`, `Nginx`, `HAProxy`, `Envoy`, `APISIX`, `Kong`) 8-vector evasion vs. zero-FP comparison (`C1`–`C3`, `B1`–`B5`).
- [`kvm_cacheline_flood.go`](kvm_cacheline_flood.go): Multi-JA4 TLS cipher-subset flooder saturating `bot_detection`'s `64`-byte `L1BitFilter` and verifying zero false positives.

## Run Against KVM (`velda-obs` -> `velda-gw:8443`)
```bash
python3 probe_bots.py 192.168.122.10:8443
go run probe_bots.go 192.168.122.10:8443
go run kvm_cross_gateway_matrix.go --gateway Velda --target 192.168.122.10:8443 --h2
go run kvm_cacheline_flood.go --target 192.168.122.10:8443 --unique-bots 2000 --concurrency 200
```
