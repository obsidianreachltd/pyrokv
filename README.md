# PyroKV

_Lightning Fast Key-Value Store_

## Overview

PyroKV is a high-performance, distributed in-memory key-value store designed for applications requiring ultra-low latency and maximum throughput. Built with modern systems programming principles, PyroKV delivers microsecond-level response times while maintaining data consistency and reliability.

### Key Features

- **Blazing Fast Performance**: Sub-millisecond read/write operations
- **Memory Optimized**: Efficient memory management with minimal overhead
- **Concurrent Access**: Lock-free data structures for high concurrency
- **Persistent Storage**: Optional durability with configurable persistence modes
- **Network Protocol**: Custom binary protocol optimized for speed
- **Clustering Support**: Built-in replication and sharding capabilities

### Use Cases

PyroKV excels in scenarios requiring:
- Real-time analytics and metrics collection
- Session storage for high-traffic applications
- Caching layer for database acceleration
- Gaming leaderboards and live statistics
- Financial trading systems requiring low-latency data access

## Running PyroKV

### Environment Variables

#### Binding a Specific Port

By default, PyroKV runs on port `8001`. To change this, use the following environment variable:

```bash
PYROKV_PORT=<your-port-number>
```

### Enabling Persistence

To enable file-based KV persistence, set the following environment variable to `true`:

```bash
PYROKV_STORAGE_ENABLED=true
```

### Docker

To run on Docker, use the following command:

**Minimal Run, Default Settings**

```bash
docker run pyrokv-server
```

This exposes the default port, `8001`

**Custom Port**

```bash
docker run -e PYROKV_PORT=9000 -p 9000:9000 pyrokv-server
```

## TODO:

* Make KVStore a single class that's referenced by each connection. Initialise with storage enabled flag.