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

**NOTE** You must also specify the port mapping in the docker run command, e.g.

```bash
docker run -p 9000:9000 -e PYROKV_PORT=9000 obsidianreachltd/pyrokv:latest
```

### Enabling Persistence

To enable file-based KV persistence, set the following environment variable to `true`:

```bash
PYROKV_STORAGE_ENABLED=true
```

### Password Authentication

To enable password authentication, set the following environment variable:

```bash
PYROKV_AUTH_PASSWORD=<my-secret-password>
```

### Docker

To run on Docker, use the following command:

#### Minimal Run, Default Settings

```bash
docker run obsidianreachltd/pyrokv:latest
```

This exposes the default port, `8001` with **no persistent storage**.

#### Custom Port

```bash
docker run -e PYROKV_PORT=9000 -p 9000:9000 obsidianreachltd/pyrokv:latest
```

#### External Volume Mounted

To run PyroKV with an external volume mounted to the container for the data, use the following:

```bash
docker run -e PYROKV_STORAGE_ENABLED=true -p 8001:8001 -v /your/volume/path:/var/lib/pyrokv obsidianreachltd/pyrokv:latest
```

#### Docker Compose Example

```yaml
services:
  pyrokv:
    image: obsidianreachltd/pyrokv:latest
    ports:
      - 8001:8001
    environment:
      - PYROKV_STORAGE_ENABLED=true
      - PYROKV_AUTH_PASSWORD=5ecr3tP@s5w0rD
    volumes:
      - pyrokv-data:/var/lib/pyrokv/data

  volumes:
    pyrokv-data
```
