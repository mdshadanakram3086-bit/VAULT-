const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { storageRoot, nodes, replicationFactor, nodeCapacity } = require("./config");

const objects = new Map();
const nodeState = new Map();
const locks = new Map();
const metadataPath = path.join(storageRoot, "metadata.json");

function sha256(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}
function nodeDir(nodeId) { return path.join(storageRoot, nodeId); }
function chunkPath(nodeId, objectId, index) {
  return path.join(nodeDir(nodeId), `${objectId}.${index}.chunk`);
}
function healthyNodes() {
  return [...nodeState.values()].filter(n => n.online && !n.partitioned);
}
function nodeCanWrite(n, size) {
  return n.online && !n.partitioned && n.capacityBytes - n.usedBytes >= size;
}
function withLock(key, fn) {
  const previous = locks.get(key) || Promise.resolve();
  let release;
  const current = new Promise(r => release = r);
  const queued = previous.then(() => current);
  locks.set(key, queued);
  return previous.then(async () => {
    try { return await fn(); }
    finally {
      release();
      if (locks.get(key) === queued) locks.delete(key);
    }
  });
}

function recalcNodeUsage() {
  for (const n of nodeState.values()) {
    const dir = nodeDir(n.id);
    let used = 0;
    if (fs.existsSync(dir)) {
      for (const name of fs.readdirSync(dir)) {
        if (!name.endsWith(".chunk")) continue;
        try {
          const st = fs.statSync(path.join(dir, name));
          if (st.isFile()) used += st.size;
        } catch (_) {}
      }
    }
    n.usedBytes = used;
    n.lastHeartbeat = new Date().toISOString();
  }
}

function serializeObject(o) {
  return {
    ...o,
    chunks: o.chunks.map(c => ({ ...c, buffer: undefined }))
  };
}
function persistMetadata() {
  const data = {
    version: 1,
    savedAt: new Date().toISOString(),
    objects: [...objects.values()].map(serializeObject)
  };
  const tmp = `${metadataPath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2));
  fs.renameSync(tmp, metadataPath);
}
function loadMetadata() {
  if (!fs.existsSync(metadataPath)) return;
  try {
    const data = JSON.parse(fs.readFileSync(metadataPath, "utf8"));
    for (const o of data.objects || []) {
      // Only retain manifests whose chunks still exist and have the recorded size/checksum.
      o.chunks = (o.chunks || []).map(c => ({ ...c, buffer: undefined }));
      objects.set(o.objectId, o);
    }
  } catch (e) {
    console.error("[Vault] metadata load failed:", e.message);
  }
}

function init() {
  fs.mkdirSync(storageRoot, { recursive: true });
  for (const n of nodes) {
    const dir = nodeDir(n.id);
    fs.mkdirSync(dir, { recursive: true });
    nodeState.set(n.id, {
      id: n.id, capacityBytes: nodeCapacity, usedBytes: 0,
      online: true, partitioned: false, lastHeartbeat: new Date().toISOString()
    });
  }
  loadMetadata();
  recalcNodeUsage();
  // Remove stale manifest replicas that no longer exist.
  for (const o of objects.values()) {
    for (const c of o.chunks) {
      c.replicas = c.replicas.filter(id => fs.existsSync(chunkPath(id, o.objectId, c.index)));
    }
  }
  persistMetadata();
}

function placementCandidates(key, exclude = []) {
  const excluded = new Set(exclude);
  return healthyNodes()
    .filter(n => !excluded.has(n.id))
    .sort((a, b) => {
      // Primary rule: least utilized node. Hash is only a stable tie breaker.
      const ua = a.usedBytes / a.capacityBytes;
      const ub = b.usedBytes / b.capacityBytes;
      if (ua !== ub) return ua - ub;
      return sha256(Buffer.from(`${key}:${a.id}`)).localeCompare(sha256(Buffer.from(`${key}:${b.id}`)));
    });
}
function chooseNodes(count, key, size, exclude = []) {
  return placementCandidates(key, exclude).filter(n => nodeCanWrite(n, size)).slice(0, count);
}

function writeChunk(nodeId, objectId, index, buffer) {
  const n = nodeState.get(nodeId);
  if (!n || !n.online || n.partitioned) throw new Error(`Node ${nodeId} unavailable`);
  const p = chunkPath(nodeId, objectId, index);
  const oldSize = fs.existsSync(p) ? fs.statSync(p).size : 0;
  const delta = buffer.length - oldSize;
  if (delta > 0 && n.capacityBytes - n.usedBytes < delta) {
    throw new Error(`Node ${nodeId} has insufficient capacity`);
  }
  fs.writeFileSync(p, buffer);
  n.usedBytes = Math.max(0, n.usedBytes + delta);
  n.lastHeartbeat = new Date().toISOString();
}
function readChunk(nodeId, objectId, index) {
  const n = nodeState.get(nodeId);
  if (!n || !n.online || n.partitioned) throw new Error(`Node ${nodeId} unavailable`);
  return fs.readFileSync(chunkPath(nodeId, objectId, index));
}
function removeChunk(nodeId, objectId, index) {
  const p = chunkPath(nodeId, objectId, index);
  if (!fs.existsSync(p)) return;
  const size = fs.statSync(p).size;
  fs.unlinkSync(p);
  const n = nodeState.get(nodeId);
  if (n) n.usedBytes = Math.max(0, n.usedBytes - size);
}
function setNodeState(id, patch) {
  const n = nodeState.get(id);
  if (!n) return null;
  Object.assign(n, patch, { lastHeartbeat: new Date().toISOString() });
  return n;
}
function listNodes() {
  return [...nodeState.values()].map(n => ({
    ...n,
    freeBytes: Math.max(0, n.capacityBytes - n.usedBytes),
    utilization: Number((n.usedBytes / n.capacityBytes * 100).toFixed(4))
  }));
}

function createManifest({objectId, originalName, mimeType, size, chunks}) {
  const now = new Date().toISOString();
  return {
    objectId, originalName, mimeType, size, version: 1,
    objectChecksum: sha256(Buffer.concat(chunks.map(c => c.buffer))),
    chunkCount: chunks.length,
    chunks: chunks.map(c => ({
      index:c.index, size:c.buffer.length, checksum:sha256(c.buffer),
      version:1, replicas:c.replicas
    })),
    createdAt:now, updatedAt:now, status:"available"
  };
}

async function putObject({objectId, originalName, mimeType, chunks, rf}) {
  return withLock(objectId, async () => {
    const existing = objects.get(objectId);
    const oldRefs = existing ? existing.chunks.flatMap(c => c.replicas.map(nodeId => ({nodeId,index:c.index}))) : [];
    const placements = [];
    try {
      const manifestChunks = [];
      for (const c of chunks) {
        const desired = Math.min(rf, healthyNodes().length);
        const selected = chooseNodes(desired, `${objectId}:${c.index}`, c.buffer.length);
        if (selected.length < desired) throw new Error("Insufficient healthy node capacity to store object with the requested replication factor");
        for (const n of selected) {
          writeChunk(n.id, objectId, c.index, c.buffer);
          placements.push({nodeId:n.id,index:c.index});
        }
        manifestChunks.push({index:c.index, buffer:c.buffer, replicas:selected.map(n=>n.id)});
      }
      const manifest = createManifest({
        objectId, originalName, mimeType,
        size:chunks.reduce((s,c)=>s+c.buffer.length,0),
        chunks:manifestChunks
      });
      manifest.replicationFactor = rf;
      manifest.version = existing ? existing.version + 1 : 1;
      if (existing) manifest.createdAt = existing.createdAt;
      objects.set(objectId, manifest);
      // Delete old version only after the replacement is safely stored.
      if (existing) for (const ref of oldRefs) removeChunk(ref.nodeId, objectId, ref.index);
      persistMetadata();
      return manifest;
    } catch (e) {
      for (const p of placements) removeChunk(p.nodeId, objectId, p.index);
      throw e;
    }
  });
}

function getObject(id) { return objects.get(id) || null; }
function getReadableChunk(record, chunk) {
  const valid = [];
  for (const nodeId of chunk.replicas || []) {
    try {
      const b = readChunk(nodeId, record.objectId, chunk.index);
      if (sha256(b) === chunk.checksum) valid.push({nodeId, buffer:b});
    } catch (_) {}
  }
  return valid;
}

async function repairChunk(record, chunk) {
  const good = getReadableChunk(record, chunk);
  if (!good.length) return {repaired:false, reason:"no valid replica", index:chunk.index};
  const goodIds = good.map(x => x.nodeId);
  const desired = Math.min(record.replicationFactor || replicationFactor, healthyNodes().length);
  const additions = chooseNodes(Math.max(0, desired - goodIds.length), `${record.objectId}:${chunk.index}`, good[0].buffer.length, goodIds);
  for (const n of additions) writeChunk(n.id, record.objectId, chunk.index, good[0].buffer);
  const before = [...chunk.replicas];
  chunk.replicas = [...goodIds, ...additions.map(n=>n.id)];
  chunk.version++;
  record.updatedAt = new Date().toISOString();
  if (additions.length) persistMetadata();
  return {repaired:additions.length > 0 || before.length !== chunk.replicas.length, index:chunk.index, replicas:chunk.replicas};
}

async function rebalanceObject(record) {
  const results = [];
  const desired = Math.min(record.replicationFactor || replicationFactor, healthyNodes().length);
  for (const chunk of record.chunks) {
    const good = getReadableChunk(record, chunk);
    if (!good.length) {
      results.push({objectId:record.objectId,index:chunk.index,moved:false,reason:"no valid replica"});
      continue;
    }
    const goodIds = good.map(x=>x.nodeId);
    const currentIds = [...new Set(chunk.replicas || [])];
    const source = good[0];
    // Fill missing replicas first, then move replicas when a healthy node is materially fuller than candidates.
    let targetIds = currentIds.filter(id => goodIds.includes(id));
    const additions = chooseNodes(desired - targetIds.length, `${record.objectId}:${chunk.index}:rebalance`, source.buffer.length, targetIds);
    for (const n of additions) {
      writeChunk(n.id, record.objectId, chunk.index, source.buffer);
      targetIds.push(n.id);
    }
    // If we have the desired count, move the fullest replica when a substantially less-utilized node exists.
    if (targetIds.length >= desired) {
      const sortedTargets = targetIds.slice().sort((a,b) => (nodeState.get(b)?.usedBytes||0) - (nodeState.get(a)?.usedBytes||0));
      for (const oldId of sortedTargets) {
        const oldNode = nodeState.get(oldId);
        const candidate = placementCandidates(`${record.objectId}:${chunk.index}:rebalance`, targetIds)
          .find(n => nodeCanWrite(n, source.buffer.length) &&
            n.usedBytes / n.capacityBytes + 0.01 < (oldNode?.usedBytes || 0) / (oldNode?.capacityBytes || 1));
        if (candidate) {
          writeChunk(candidate.id, record.objectId, chunk.index, source.buffer);
          targetIds = targetIds.filter(id => id !== oldId);
          targetIds.push(candidate.id);
          removeChunk(oldId, record.objectId, chunk.index);
          results.push({objectId:record.objectId,index:chunk.index,moved:true,from:oldId,to:candidate.id});
          break;
        }
      }
    }
    if (additions.length) results.push({objectId:record.objectId,index:chunk.index,moved:true,added:additions.map(n=>n.id)});
    chunk.replicas = targetIds;
    chunk.version++;
    record.updatedAt = new Date().toISOString();
  }
  return results;
}

async function repairAll(objectId) {
  const targets = objectId ? [objects.get(objectId)].filter(Boolean) : [...objects.values()];
  const results = [];
  for (const record of targets) {
    for (const chunk of record.chunks) results.push({objectId:record.objectId, ...(await repairChunk(record, chunk))});
  }
  return results;
}
async function rebalanceAll(objectId) {
  const targets = objectId ? [objects.get(objectId)].filter(Boolean) : [...objects.values()];
  const results = [];
  for (const record of targets) results.push(...await rebalanceObject(record));
  if (results.length) persistMetadata();
  return results;
}

function verifyObject(objectId) {
  const record = objects.get(objectId);
  if (!record) return {objectId,healthy:false,reason:"metadata not found"};
  const chunks = record.chunks.map(c => {
    const replicas = (c.replicas || []).map(nodeId => {
      try {
        const b = readChunk(nodeId,record.objectId,c.index);
        return {nodeId,healthy:sha256(b)===c.checksum,checksum:sha256(b)};
      } catch(e) { return {nodeId,healthy:false,error:e.message}; }
    });
    return {index:c.index,expectedChecksum:c.checksum,healthy:replicas.some(r=>r.healthy),replicas};
  });
  return {objectId,version:record.version,chunkCount:record.chunkCount,healthy:chunks.every(c=>c.healthy),chunks};
}
function allObjects() { return [...objects.values()]; }
function deleteObject(id) {
  const record=objects.get(id);
  if (!record) return false;
  for (const c of record.chunks) for (const nodeId of c.replicas || []) removeChunk(nodeId,id,c.index);
  objects.delete(id); persistMetadata(); return true;
}
function corruptChunk(nodeId,objectId,index) {
  const p=chunkPath(nodeId,objectId,Number(index));
  if(!nodeState.has(nodeId)) return {error:"node not found"};
  if(!fs.existsSync(p)) return {error:"chunk not found on node"};
  const b=fs.readFileSync(p); if(!b.length) return {error:"empty chunk"};
  b[0]^=0xff; fs.writeFileSync(p,b);
  return {nodeId,objectId,index:Number(index),corrupted:true};
}
function stats() {
  const ns=listNodes();
  return {
    objectCount:objects.size,nodeCount:ns.length,
    healthyNodes:ns.filter(n=>n.online&&!n.partitioned).length,
    failedNodes:ns.filter(n=>!n.online).length,
    partitionedNodes:ns.filter(n=>n.partitioned).length,
    totalCapacityBytes:ns.reduce((s,n)=>s+n.capacityBytes,0),
    usedBytes:ns.reduce((s,n)=>s+n.usedBytes,0),
    freeBytes:ns.reduce((s,n)=>s+n.freeBytes,0),
    averageUtilization:Number((ns.reduce((s,n)=>s+n.utilization,0)/ns.length).toFixed(4)),
    replicationFactor
  };
}
module.exports={
  init,sha256,healthyNodes,putObject,getObject,getReadableChunk,repairChunk,repairAll,
  rebalanceAll,verifyObject,allObjects,deleteObject,setNodeState,listNodes,corruptChunk,
  stats,readChunk,writeChunk,removeChunk
};
