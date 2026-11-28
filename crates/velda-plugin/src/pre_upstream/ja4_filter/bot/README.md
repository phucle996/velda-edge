# Direct KVM `ja4_filter` Request Spoofing Probes

Simple request-spoofing script hitting `velda-gw` (`192.168.122.10:8443` by default) to verify **`ja4_filter`** on live KVM traffic.

## Files
- [`probe_ja4.py`](probe_ja4.py): Sends HTTPS requests spoofing full Chrome headers using two distinct OpenSSL TLS cipher configurations (`default` vs `TLSv1.2 ECDHE-ECDSA-AES128-GCM-SHA256`).

## Run Against KVM (`velda-obs` -> `velda-gw:8443`)
```bash
python3 probe_ja4.py 192.168.122.10:8443
# When Python default JA4 is added to `ja4_filter.deny`:
python3 probe_ja4.py 192.168.122.10:8443 --expect-denied-default
```
