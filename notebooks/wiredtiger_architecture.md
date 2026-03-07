# WiredTiger Architecture Diagram

## Class Relationship Overview

```mermaid
classDiagram
    WT_CONNECTION_IMPL "-->" "1..*" WT_SESSION_IMPL : creates
    WT_SESSION_IMPL "1" --> "1" WT_TXN : contains
    WT_SESSION_IMPL "1" --> "1..*" WT_CURSOR : creates

    WT_CURSOR "*" --> "1" WT_SESSION_IMPL : session (CUR2S)
    WT_CURSOR_BTREE --|> WT_CURSOR
    WT_CURSOR_BTREE "1" --> "1" WT_BTREE : operates on

    WT_BTREE "*" --> "1" WT_CONNECTION_IMPL : owned by
    WT_BTREE "1" --> "1..*" WT_REF : root/reference
    WT_REF "*" --> "1" WT_PAGE : points to

    WT_PAGE "1" --> "0..*" WT_MODIFY : modifications
    WT_PAGE "1" --> "0..*" WT_UPDATE : update chain

    class WT_CONNECTION_IMPL {
        +WT_SESSION_IMPL* default_session
        +WT_BTREE* btrees
        open_session()
        open_cursor()
    }

    class WT_SESSION_IMPL {
        +WT_TXN* txn
        +WT_CONNECTION_IMPL* connection
        +WT_CURSOR* cursor_cache
        begin_transaction()
        commit_transaction()
        open_cursor(uri, config)
    }

    class WT_TXN {
        +uint64_t id
        +WT_TXN_ISOLATION isolation
        +uint64_t snapshot_min
        +wt_timestamp_t read_timestamp
        +bool running
        visible(txn_id, timestamp)
    }

    class WT_CURSOR {
        +WT_SESSION_IMPL* session
        +WT_ITEM key
        +WT_ITEM value
        <<interface>>
    }

    class WT_CURSOR_BTREE {
        +WT_CURSOR iface
        +WT_REF* ref
        +WT_BTREE* btree
        +int compare
        search(key)
        next()
        reset()
    }

    class WT_BTREE {
        +WT_SESSION_IMPL* session
        +WT_REF* root
        +uint32_t pagemax
        key_format
        value_format
    }

    class WT_REF {
        +WT_PAGE* page
        +wt_timestamp_t rec_max_txn
        +uint8_t state
        +addr_t addr
    }

    class WT_PAGE {
        +WT_PAGE_HEADER* header
        +void* dsk
        +WT_MODIFY* modify
        +u_int type
    }

    class WT_MODIFY {
        +WT_UPDATE* first_update
        +WT_UPDATE* last_update
        +uint32_t write_gen
    }

    class WT_UPDATE {
        +WT_TXN* txn
        +wt_timestamp_t start_ts
        +wt_timestamp_t durable_ts
        +WT_UPDATE* next
        +data/value
    }
```

## Key Navigation Macros

```mermaid
graph LR
    CURSOR[WT_CURSOR_BTREE* cbt] -->|"CUR2S(cbt)"| SESSION[WT_SESSION_IMPL* session]
    SESSION -->|"session->txn"| TXN[WT_TXN* txn]
    SESSION -->|"S2BT(session)"| BTREE[WT_BTREE* btree]

    style CURSOR fill:#e1f5ff
    style SESSION fill:#fff4e1
    style TXN fill:#ffe1e1
    style BTREE fill:#e1ffe1
```

## Cursor Lifecycle Flow

```mermaid
sequenceDiagram
    participant RU as RecoveryUnit
    participant WS as WiredTigerSession
    participant WC as WiredTigerCursor
    participant CUR as WT_CURSOR_BTREE
    participant BT as WT_BTREE
    participant TX as WT_TXN

    RU->>WS: getSession()
    WS->>WS: _ensureSession()
    RU->>WS: open_cursor(uri, btree)

    Note over WS,CUR: CURSOR FACTORY PATTERN
    WS->>CUR: wt_session->open_cursor(uri)
    CUR->>BT: btree = S2BT(session)
    CUR->>CUR: initialize cursor state

    RU->>WC: new WiredTigerCursor(uri, session, btree)
    WC->>CUR: getNewCursor(uri)

    Note over WC,CUR: READ OPERATION
    RU->>WC: cursor->search(key)
    WC->>CUR: cursor->search(key)
    CUR->>CUR: session = CUR2S(cbt)
    CUR->>BT: __wt_row_search(cbt, key)
    BT->>TX: __wt_txn_visible(session, txn_id)
    TX-->>BT: visible?
    BT-->>CUR: result
    CUR-->>WC: record
    WC-->>RU: Record
```

## Memory Layout

```mermaid
graph TB
    subgraph CONNECTION["WT_CONNECTION_IMPL"]
        direction TB
        S1[WT_SESSION_IMPL]
        S2[WT_SESSION_IMPL]
        S3[...cached sessions]
        BT1[WT_BTREE]
        BT2[WT_BTREE]
    end

    subgraph SESSION["WT_SESSION_IMPL"]
        direction LR
        TXN[WT_TXN]
        CUR_CACHE[cursor cache]
    end

    subgraph CURSOR["WT_CURSOR_BTREE"]
        direction LR
        IFACE[WT_CURSOR iface]
        REF[WT_REF* ref]
    end

    subgraph PAGE["WT_PAGE"]
        direction TB
        MOD[WT_MODIFY]
        UPDATES[WT_UPDATE chain]
    end

    CONNECTION --> SESSION
    SESSION --> CURSOR
    CURSOR --> PAGE

    style TXN fill:#ffcccc
    style CUR_CACHE fill:#ccccff
    style UPDATES fill:#ccffcc
```

## Visibility Check Path

```mermaid
graph TD
    START[cursor->next/search] --> CUR2S[CUR2S get session]
    CUR2S --> BT_OP[__wt_row_search/cursor operation]
    BT_OP --> PAGE_IN[__wt_page_in_func]
    PAGE_IN --> GET_UPDATES[get update chain from page]
    GET_UPDATES --> VISIBLE[__wt_txn_visible]

    subgraph VISIBILITY["Transaction Visibility Check"]
        VISIBLE --> CHECK_ID{txn_id visible?}
        CHECK_ID -->|yes| CHECK_TS{timestamp visible?}
        CHECK_ID -->|no| SKIP[skip this version]
        CHECK_TS -->|yes| RETURN[return this version]
        CHECK_TS -->|no| SKIP
    end

    SKIP --> NEXT_VERSION[next update in chain]
    NEXT_VERSION --> VISIBLE

    style VISIBLE fill:#ffcccc
    style RETURN fill:#ccffcc
    style SKIP fill:#cccccc
```
