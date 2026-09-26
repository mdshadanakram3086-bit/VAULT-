const path = require("path");

const envInt = (name, fallback) => {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : fallback;
};

module.exports = {
  port: envInt("PORT", 5000),
  replicationFactor: Math.max(1, envInt("REPLICATION_FACTOR", 3)),
  chunkSize: Math.max(64 * 1024, envInt("CHUNK_SIZE_BYTES", 1024 * 1024)),
  nodeCapacity: Math.max(1024 * 1024, envInt("NODE_CAPACITY_BYTES", 10 * 1024 * 1024 * 1024)),
  maxUploadBytes: Math.max(1024 * 1024, envInt("MAX_UPLOAD_BYTES", 1024 * 1024 * 1024)),
  repairIntervalMs: envInt("REPAIR_INTERVAL_MS", 10000),
  rebalanceIntervalMs: envInt("REBALANCE_INTERVAL_MS", 30000),
  maxConcurrentUploads: Math.max(1, envInt("MAX_CONCURRENT_UPLOADS", 20)),
  storageRoot: path.resolve(process.env.STORAGE_ROOT || "./data/nodes"),
  nodes: Array.from({length: 6}, (_, i) => ({
    id: `node-${i+1}`
  }))
};