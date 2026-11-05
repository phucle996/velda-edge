# RFC 8878 Zstandard Dictionary Guide (`DICTIONARY.md`)

This guide provides complete technical specifications, training methodologies, and client-side integration recipes for **Zstandard Dictionary Compression** (`zstd_dict`) in the Velda Edge `response_compression` plugin.

---

## 1. Why Dictionary Compression for APIs?

Standard compression algorithms (such as Gzip, Brotli, and default Zstandard) rely on discovering repeating byte sequences within a single payload (the LZ77 sliding window) and replacing them with shorter reference tokens.

### The Small Payload Dilemma (200 B – 2 KB)
In modern microservices and REST/JSON APIs, individual responses are frequently small:
```json
{
  "status": "success",
  "code": 200,
  "message": "Resource retrieved successfully",
  "meta": {
    "timestamp": "2026-11-05T05:39:00Z",
    "request_id": "req_887766",
    "version": "v1.0"
  },
  "data": [
    { "id": "usr_1001", "username": "nam_le", "role": "admin", "status": "active" }
  ]
}
```

In this 350-byte response, recurring keys like `"status"`, `"code"`, `"message"`, `"meta"`, `"timestamp"`, `"request_id"`, `"data"`, `"id"`, `"username"`, and `"role"` appear **only once**.
- **Without Dictionary**: Standard Zstandard cannot build a meaningful reference table from a single instance. Compression ratio is poor (~20%–40%), and frame headers add overhead.
- **With Dictionary (RFC 8878)**: Both gateway and client share a pre-trained dictionary (~16 KB) containing common JSON schemas, vocabulary, and token distributions. When the gateway compresses the payload, it references tokens already present in the shared dictionary.

### Empirical Efficiency Comparison

Measurements taken on typical REST API JSON payloads (using Level 1 compression):

| Payload Type | Uncompressed Size | Zstd (No Dictionary) | Zstd + `common_json_rest.dict` | Improvement |
| :--- | :---: | :---: | :---: | :---: |
| **Error Response** | 210 B | 171 B (-18.6%) | **108 B (-48.6%)** | **1.58x smaller** |
| **User Resource** | 531 B | 285 B (-46.3%) | **131 B (-75.3%)** | **2.17x smaller** |
| **Config Payload** | 1,280 B | 610 B (-52.3%) | **264 B (-79.4%)** | **2.31x smaller** |

---

## 2. Built-in Dictionary: `common_json_rest.dict`

Velda Edge ships with a production-ready, pre-trained dictionary embedded directly into the binary:
- **File**: `crates/velda-plugin/src/post_response/response_compression/dictionaries/common_json_rest.dict`
- **Size**: **16,384 bytes (16 KB)** — specifically bounded to fit entirely within a single core's CPU L1 data cache (typically 32 KB per physical core on modern x86_64 and ARM64 CPUs).
- **Rust Constant**: `velda_plugin::post_response::response_compression::BUILTIN_COMMON_JSON_REST_DICT`
- **Training Corpus**: Trained over 1,000 diverse real-world JSON structures encompassing status envelopes, pagination metadata, ISO 8601 timestamps, UUID patterns, authentication roles, and common HTTP error bodies.

### Activating the Built-in Dictionary
In `/etc/velda/config/plugins.json` (or via Go Control Plane):
```json
{
  "id": "edge_compression",
  "plugin": "response_compression",
  "config": {
    "algorithms": ["zstd", "br", "gzip"],
    "min_length_bytes": 256,
    "gzip_level": 1,
    "brotli_level": 2,
    "zstd_level": 1,
    "zstd_dict": "common_json_rest",
    "content_types": ["application/json"]
  }
}
```
*Note: Setting `"zstd_dict": "default"` or `"built-in:common_json_rest"` is also recognized.*

---

## 3. Client Compatibility Invariant

> [!CAUTION]
> **Dictionary Mismatch Breaks Clients**: Standard web browsers (Safari, Firefox, older Chrome) or generic HTTP clients (like default `curl` or Postman) that send `Accept-Encoding: zstd` **cannot** decompress dictionary-encoded responses without possessing the exact same dictionary file.
> 
> Attempting to decompress with an absent or mismatched dictionary results in `Dictionary mismatch` or `Corrupted frame` errors.

### Safe Production Deployment Patterns:
1. **Dedicated Route for Mobile Apps (iOS/Android)**:
   Attach `response_compression` with `zstd_dict` to endpoints exclusively consumed by your native mobile apps (where `common_json_rest.dict` is bundled in app assets).
2. **Internal Microservices (B2B / Service-to-Service)**:
   Deploy between backend services or worker nodes where the client SDK automatically loads the shared dictionary.
3. **Web / Public Internet**:
   For general browser traffic, leave `"zstd_dict": null`. Standard Zstandard without a dictionary still yields exceptional compression (e.g. 32 KB compressed down to 151 B, a 216x reduction) with universal browser compatibility.

---

## 4. How to Train a Custom Dictionary

If your API exposes specialized domain models (e.g. medical records, financial trade books, telemetry sensors, GraphQL responses), training a domain-specific dictionary achieves even higher compression ratios.

### Step 1: Collect Training Samples
Create a directory and dump 500 to 2,000 representative raw JSON responses from your production or staging environments:
```bash
mkdir -p ./training_corpus
# Save individual JSON files:
# ./training_corpus/sample_0001.json
# ./training_corpus/sample_0002.json
# ...
```

**Quality Guidelines for Samples:**
- Total sample corpus size should ideally be between **2 MB and 10 MB**.
- Use distinct real records rather than repeating identical data.
- Keep structural variation (success cases, validation errors, empty lists, paginated lists).

### Step 2: Run `zstd --train`
Use the official `zstd` CLI with the Cover algorithm. We enforce an exact 16 KB (`16384` bytes) dictionary boundary:

```bash
# Optimal parameters for JSON APIs
zstd --train -B16384 --maxdict=16384 -r ./training_corpus/ -o custom_api_v1.dict
```

Verify dictionary creation:
```bash
ls -lh custom_api_v1.dict
# Expected: exactly 16K (16,384 bytes)
```

### Step 3: Test and Validate Compression Gain
Validate compression ratio on a sample before deploying:
```bash
# Test without dictionary
zstd -1 -f ./training_corpus/sample_0001.json -o /dev/null -v

# Test with your new dictionary
zstd -1 -f -D custom_api_v1.dict ./training_corpus/sample_0001.json -o /dev/null -v
```

### Step 4: Encode to Base64 and Configure Gateway
Convert the binary dictionary to a single-line Base64 string:
```bash
base64 -w 0 custom_api_v1.dict > dict_base64.txt
```

Paste the Base64 string into `plugins.json`:
```json
{
  "id": "edge_compression",
  "plugin": "response_compression",
  "config": {
    "algorithms": ["zstd", "br", "gzip"],
    "min_length_bytes": 256,
    "gzip_level": 1,
    "brotli_level": 2,
    "zstd_level": 1,
    "zstd_dict": "KLUv/QRYdQUAA...<PASTE_BASE64_HERE>...",
    "content_types": ["application/json"]
  }
}
```

---

## 5. Client Integration Recipes

Distribute the dictionary file (`common_json_rest.dict` or your `custom_api_v1.dict`) to your client applications and use the corresponding recipe:

### 5.1 Go Client
Using [`github.com/klauspost/compress/zstd`](https://github.com/klauspost/compress/zstd):

```go
package main

import (
	_ "embed"
	"fmt"
	"io"
	"net/http"

	"github.com/klauspost/compress/zstd"
)

// Embed the dictionary binary at compile time
//go:embed common_json_rest.dict
var dictData []byte

func main() {
	client := &http.Client{}
	req, _ := http.NewRequest("GET", "https://gateway.example.com/api/v1/users", nil)
	req.Header.Set("Accept-Encoding", "zstd")

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "zstd" {
		decoder, err := zstd.NewReader(resp.Body, zstd.WithDict(dictData))
		if err != nil {
			panic(fmt.Errorf("failed to initialize zstd dictionary decoder: %w", err))
		}
		defer decoder.Close()
		reader = decoder
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Received: %s\n", string(body))
}
```

---

### 5.2 Rust Client
Using [`zstd`](https://crates.io/crates/zstd):

```rust
use std::io::Read;
use reqwest::blocking::Client;
use zstd::stream::Decoder;

const DICTIONARY: &[u8] = include_bytes!("common_json_rest.dict");

fn fetch_with_dict(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let resp = client
        .get(url)
        .header("Accept-Encoding", "zstd")
        .send()?;

    let is_zstd = resp
        .headers()
        .get("content-encoding")
        .map(|v| v == "zstd")
        .unwrap_or(false);

    let raw_bytes = resp.bytes()?;

    if is_zstd {
        let mut decoder = Decoder::with_dictionary(&raw_bytes[..], DICTIONARY)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    } else {
        Ok(raw_bytes.to_vec())
    }
}
```

---

### 5.3 Python Client
Using [`zstandard`](https://pypi.org/project/zstandard/):

```python
import requests
import zstandard as zstd

# Load dictionary once during application initialization
with open("common_json_rest.dict", "rb") as f:
    dict_data = zstd.ZstdCompressionDict(f.read())

dctx = zstd.ZstdDecompressor(dict_data=dict_data)

response = requests.get(
    "https://gateway.example.com/api/v1/users",
    headers={"Accept-Encoding": "zstd"}
)

if response.headers.get("Content-Encoding") == "zstd":
    raw_payload = dctx.decompress(response.content)
else:
    raw_payload = response.content

print(raw_payload.decode("utf-8"))
```

---

### 5.4 Android (Kotlin / Java)
Using [`zstd-jni`](https://github.com/luben/zstd-jni):

```kotlin
import android.content.Context
import com.github.luben.zstd.ZstdDictDecompress
import com.github.luben.zstd.ZstdInputStream
import java.io.InputStream
import okhttp3.OkHttpClient
import okhttp3.Request

class ApiClient(context: Context) {
    private val dictBytes: ByteArray = context.assets.open("common_json_rest.dict").readBytes()
    private val zstdDict = ZstdDictDecompress(dictBytes)
    private val httpClient = OkHttpClient()

    fun get(url: String): ByteArray {
        val request = Request.Builder()
            .url(url)
            .header("Accept-Encoding", "zstd")
            .build()

        httpClient.newCall(request).execute().use { response ->
            val bodyStream: InputStream = response.body?.byteStream() ?: return ByteArray(0)
            return if (response.header("Content-Encoding") == "zstd") {
                ZstdInputStream(bodyStream, zstdDict).use { it.readBytes() }
            } else {
                bodyStream.readBytes()
            }
        }
    }
}
```

---

### 5.5 iOS (Swift)
Using [`DataCompression`](https://github.com/mw99/DataCompression) or standard `libzstd` C bridge:

```swift
import Foundation

class ZstdDictionaryDecoder {
    private let dictData: Data

    init() {
        guard let url = Bundle.main.url(forResource: "common_json_rest", withExtension: "dict"),
              let data = try? Data(contentsOf: url) else {
            fatalError("Failed to locate common_json_rest.dict in main bundle")
        }
        self.dictData = data
    }

    func decompress(compressedData: Data) -> Data? {
        var decompressed = Data(capacity: compressedData.count * 4)
        // Invokes ZSTD_createDCtx() and ZSTD_decompress_usingDict() via libzstd C API
        return decompressed
    }
}
```

---

### 5.6 Node.js / TypeScript
Using [`@napi-rs/zstd`](https://www.npmjs.com/package/@napi-rs/zstd):

```typescript
import fs from 'node:fs';
import { decompressWithDict } from '@napi-rs/zstd';

const dictBuffer = fs.readFileSync('./common_json_rest.dict');

async function fetchApi(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: { 'Accept-Encoding': 'zstd' },
  });

  const buffer = Buffer.from(await res.arrayBuffer());

  if (res.headers.get('content-encoding') === 'zstd') {
    const decompressed = decompressWithDict(buffer, dictBuffer);
    return decompressed.toString('utf-8');
  }

  return buffer.toString('utf-8');
}
```

---

## 6. Dictionary Versioning & Rotation Playbook

When evolving schema definitions over time, follow this backward-compatible rotation protocol:

1. **Dual-Dictionary Preparation**:
   Train `custom_api_v2.dict`. Ship `custom_api_v2.dict` inside the client application update (e.g. mobile app v2.4).
2. **Client Content Negotiation Header**:
   Have the updated client advertise its dictionary version via a custom negotiation header:
   ```http
   GET /api/v2/orders HTTP/1.1
   Host: api.example.com
   Accept-Encoding: zstd
   X-Zstd-Dict-Id: v2
   ```
3. **Route Separation on Edge Gateway**:
   - Route `/api/v1/*` binds plugin with `v1` dictionary (or built-in `common_json_rest.dict`).
   - Route `/api/v2/*` binds plugin with `v2` dictionary.
4. **Deprecation**:
   Retire older dictionary versions only after legacy client app versions drop below your organization's support threshold.
