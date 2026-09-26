# VAULT — Full-Stack Distributed Object Storage Demo

Hackathon prototype implementing the requested Vault features on one machine.

## Requested features covered

### 1. Self-repair after corruption
Each chunk has a SHA-256 checksum. On verification/read, bad or missing replicas are detected. A valid replica is copied to a healthy node automatically.

### 2. Self-replication
Every chunk is placed on a configurable number of independent nodes (`REPLICATION_FACTOR`, default 3).

### 3. Large-file chunking
Files are split into fixed-size chunks (`CHUNK_SIZE_BYTES`, default 1 MiB). Each chunk has its own checksum and replica set.

### 4. 100+ concurrent users
The API is asynchronous and stateless at the HTTP layer. Upload concurrency is bounded by a semaphore, and chunk operations are independent. For a real production deployment, run multiple API processes behind a load balancer and use a durable metadata service.

### 5. Network partition simulation
Nodes have independent `online` and `partitioned` state. A partitioned node cannot exchange data with other nodes but still retains its local data.

### 6. Metadata / data extraction
`/api/objects/:id/metadata` exposes object size, MIME type, checksum, chunk count, chunk checksums, replica placement, versions, timestamps and status.

### 7. Redistributing / rebalancing
The rebalance endpoint finds under-replicated chunks and places them on eligible nodes. It also reports node utilization.

### 8. Node independence
No object depends on one specific node. A read can reconstruct the object from chunks stored on different nodes. Nodes do not need to be mutually available for the object to remain readable.

### 9. Replica consistency
Chunk checksum and version are stored in metadata. Reads only accept a replica whose checksum matches metadata. Repair copies a verified chunk from a good replica.

## Run

```bash
npm install
npm start
```

Open:

`http://localhost:5000/api/health`

## Upload a large file

```bash
curl -X POST http://localhost:5000/api/objects \
  -F "file=@./sample.txt" \
  -F "replicationFactor=3"
```

Response contains the object ID and full chunk metadata.

## Download / reconstruct

```bash
curl http://localhost:5000/api/objects/<OBJECT_ID> -o recovered.bin
```

The server retrieves each chunk from any healthy verified replica, in parallel, and concatenates them in chunk order.

## Metadata

```bash
curl http://localhost:5000/api/objects/<OBJECT_ID>/metadata
```

## Node dashboard

```bash
curl http://localhost:5000/api/nodes
```

## Simulate failure

```bash
curl -X POST http://localhost:5000/api/nodes/node-2/fail
```

## Simulate network partition

```bash
curl -X POST http://localhost:5000/api/nodes/node-3/partition
```

Recover:

```bash
curl -X POST http://localhost:5000/api/nodes/node-3/recover
```

## Simulate corruption

```bash
curl -X POST http://localhost:5000/api/nodes/node-1/corrupt/<OBJECT_ID>/<CHUNK_INDEX>
```

The next verification/repair cycle will detect the bad checksum and repair the chunk from another replica.

## Force verification + repair

```bash
curl -X POST http://localhost:5000/api/repair
```

## Rebalance

```bash
curl -X POST http://localhost:5000/api/rebalance
```

## Verify all data

```bash
curl -X POST http://localhost:5000/api/verify
```

## Delete

```bash
curl -X DELETE http://localhost:5000/api/objects/<OBJECT_ID>
```

## Architecture

```text
                   Canva UI / Browser
                          |
                    REST API Layer
                          |
                 +--------+--------+
                 | Metadata Store  |
                 | Object Manifest |
                 +--------+--------+
                          |
                    Placement Layer
                          |
       +------------------+------------------+
       |                  |                  |
    Node-1             Node-2             Node-3 ...
    chunks              chunks              chunks
       |                  |                  |
       +---------- independent replicas -----+

Object
  |
  +-- Chunk 0 -> Node 1, Node 4, Node 5
  +-- Chunk 1 -> Node 2, Node 3, Node 5
  +-- Chunk 2 -> Node 1, Node 2, Node 4
```

## Important limitation for the hackathon

This is a single-process demonstrator. "Distributed nodes" are simulated as independent directories. Metadata is in memory and is therefore lost if the server restarts.

For a production version:
- use PostgreSQL/etcd/Consul for strongly consistent metadata
- use real independent storage machines/containers
- use a durable queue for repair/rebalance jobs
- use heartbeats and network timeouts
- use resumable/multipart uploads
- use erasure coding for lower storage overhead
- use object/version locking or conditional writes
- use multiple API workers behind a load balancer
- use durable metadata snapshots/WAL

The prototype intentionally keeps these components local so it can be demonstrated quickly in a hackathon.


## Integrated UI

The Canva-style UI is included in `public/index.html` and is served by the same Express server.

Run:

```bash
npm install
npm start
```

Then open:

`http://localhost:5000`

The dashboard automatically reads live backend data.

### UI actions connected to backend

- Upload -> `POST /api/objects`
- Download -> `GET /api/objects/:id`
- Metadata -> `GET /api/objects/:id/metadata`
- Verify object -> `GET /api/objects/:id/verify`
- Verify all -> `POST /api/verify`
- Repair -> `POST /api/repair`
- Rebalance -> `POST /api/rebalance`
- Node fail/recover -> node APIs
- Network partition/heal -> node APIs
- Dashboard stats -> `/api/health`, `/api/stats`, `/api/nodes`, `/api/objects`

The circular health percentage and node capacity values are populated from the backend rather than the original Canva demo numbers.


## Upload troubleshooting

The Objects page uses a real file picker and drag-and-drop. The upload request is sent directly to the same Vault server at `POST /api/objects`.

If an upload fails:
1. Make sure `npm start` is still running.
2. Open `http://localhost:5000/api/health`.
3. Make sure at least 3 nodes are healthy when using the default 3x replication policy.
4. The default maximum upload is 500 MB (`MAX_UPLOAD_BYTES`).
5. The browser will show the backend error as a toast message.


## Upload diagnostic

Open:

`http://localhost:5000/api/upload-status`

It should return `uploadReady: true` and the configured upload limit. The default limit in this build is 1 GB.

The upload UI uses a real browser file picker plus XMLHttpRequest progress reporting, so a 1.3 MB file should be accepted easily.

## Upload fixes in v2.1
- Removed the old Canva demo upload handler so file picker and drag/drop use only the real backend.
- Added upload cancellation, timeout and network-error handling.
- Upload progress now reflects the actual XMLHttpRequest upload.
- Backend validates replication factor and returns clearer upload errors.
- Downloads now preserve the original filename.
- Default node capacity is 10 GB per node. Set `NODE_CAPACITY_BYTES=107374182400` explicitly if you use a `.env` file.
- For a fresh run, delete `data/nodes` if you want to reset all stored chunks.
