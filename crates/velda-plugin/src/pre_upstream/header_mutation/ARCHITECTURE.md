# Header Mutation Plugin (`header_mutation`) — Architectural Specification

This document details the internal architecture, zero-allocation algorithms, memory layout, and request transformation pipeline of the **`header_mutation`** plugin in Velda Edge.

---

## 1. Architectural Philosophy & Invariants

The `header_mutation` plugin operates at the **`pre_upstream`** pipeline phase immediately after route matching and load balancing, transforming the outgoing `L7Request` before upstream socket transmission.

```mermaid
flowchart TD
    Req["Incoming L7Request"] --> HopByHop{"strip_hop_by_hop = true?"}
    
    HopByHop -->|Yes| FastHop["1. Combined CONNECTION check & removal\n2. Strip custom connection-listed tokens\n3. Remove RFC Hop-by-Hop headers"]
    HopByHop -->|No| RemoveBlacklist
    FastHop --> RemoveBlacklist
    
    RemoveBlacklist["Remove Blacklisted Headers\n(remove_headers: O(1) HeaderMap probes)"]
    
    RemoveBlacklist --> Rename["Rename Headers In-Place\n• Single-value fast path (99.99%)\n• Anti-shadowing destination cleanup"]
    
    Rename --> StaticSet["Static Sets\n(100% direct insert, 0 match branches)"]
    
    StaticSet --> StaticAppend["Static Appends\n(100% direct append)"]
    
    StaticAppend --> DynamicCheck{"Any Dynamic Variables?"}
    DynamicCheck -->|Yes| DynCtx["DynamicContext (Stack-Cached)\n• $client_ip (cached on first resolve)\n• $scheme (cached on first resolve)\n• $request_id (W3C Slot 0)\n• $host, $method, $path"]
    DynamicCheck -->|No| Finish["Action::Continue\n(Forward to Upstream)"]
    
    DynCtx --> Finish
```

### Core Invariants:
1. **Pre-Partitioned Static & Dynamic Arrays**: Static header specifications are segregated into `static_sets` and `static_appends` during initialization. The request hot path inserts them directly into `HeaderMap` without executing `match` branching.
2. **Single-Pass Hop-by-Hop Removal**: `headers.remove(http::header::CONNECTION)` combines the presence check and table removal in a single hash probe. In clean HTTP/2, HTTP/3, or keep-alive requests without `Connection`, token splitting is completely bypassed.
3. **Anti-Hop-by-Hop Strip Abuse (CVE-2022-26143)**: Fast length-bucket dispatch guarantees that essential protocol headers (`Host`, `Authorization`, `x-request-id`, `x-real-ip`, etc.) cannot be deleted by client-supplied `Connection` tokens.
4. **Anti-Shadowing Header Renaming**: Before a header is renamed from `source` to `target`, the destination `target` is wiped clean to prevent malicious client-injected shadow headers.
5. **Stack-Cached Dynamic Context**: When a request references dynamic variables (e.g. both `x-real-ip` and `x-forwarded-for` referencing `$client_ip`), resolution runs once and caches the `HeaderValue` on the stack, eliminating redundant string operations.
6. **Zero Allocation for Static Headers**: Pre-computes `HeaderName` and `HeaderValue` during configuration compilation, requiring zero heap allocation during request serving.

---

## 2. Memory Layout & Data Structures

```mermaid
classDiagram
    class HeaderMutationPlugin {
        +String id
        +HeaderMutationConfig config
        -Vec~HeaderName, HeaderValue~ static_sets
        -Vec~HeaderName, DynamicVar~ dynamic_sets
        -Vec~HeaderName, HeaderValue~ static_appends
        -Vec~HeaderName, DynamicVar~ dynamic_appends
    }

    class HeaderMutationConfig {
        +bool strip_hop_by_hop
        +Vec~HeaderName~ remove
        +Vec~HeaderName, HeaderName~ rename
        +Vec~HeaderName, HeaderValueSpec~ set
        +Vec~HeaderName, HeaderValueSpec~ append
    }

    class HeaderValueSpec {
        <<enumeration>>
        Static(HeaderValue)
        Variable(DynamicVar)
    }

    class DynamicVar {
        <<enumeration>>
        ClientIp
        RequestId
        Scheme
        Host
        Method
        Path
    }

    HeaderMutationPlugin *-- HeaderMutationConfig
    HeaderMutationConfig *-- HeaderValueSpec
    HeaderValueSpec *-- DynamicVar
```

---

## 3. Hot-Path Complexity & Performance

- **Static Insertion / Removal**: $O(1)$ amortized hash table lookup via `http::HeaderMap`.
- **Hop-by-Hop Stripping**: $O(K)$ where $K$ is the number of hop-by-hop headers present (typically 0 or 1).
- **Dynamic Context**: $O(1)$ stack allocation, caching `client_ip` and `scheme` pointers.
