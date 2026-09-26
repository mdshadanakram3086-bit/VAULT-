const express = require("express");
const cors = require("cors");
const multer = require("multer");
const crypto = require("crypto");
const path = require("path");
const {
  port, replicationFactor, chunkSize, maxUploadBytes,
  repairIntervalMs, rebalanceIntervalMs, maxConcurrentUploads
} = require("./config");
const store = require("./store");
const Semaphore = require("./limiter");

store.init();

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: maxUploadBytes, files: 1 },
  fileFilter: (req, file, cb) => {
    if (!file || !file.originalname) return cb(new Error("Invalid file"));
    cb(null, true);
  }
});
const uploadLimiter = new Semaphore(maxConcurrentUploads);

app.use(cors({ origin: true }));
app.use(express.json({limit:"2mb"}));
app.use(express.static(path.join(__dirname, "../public")));

function splitBuffer(buffer) {
  const chunks = [];
  let index = 0;
  for (let offset=0; offset<buffer.length; offset += chunkSize) {
    chunks.push({
      index,
      buffer: buffer.subarray(offset, Math.min(offset + chunkSize, buffer.length))
    });
    index++;
  }
  return chunks;
}

app.get("/", (req,res) => res.sendFile(path.join(__dirname, "../public/index.html")));

app.get("/api/health", (req,res) => {
  const s = store.stats();
  res.json({
    status: s.healthyNodes > 0 ? "healthy" : "unavailable",
    ...s,
    timestamp:new Date().toISOString()
  });
});

app.get("/api/stats",(req,res)=>res.json(store.stats()));
app.get("/api/nodes",(req,res)=>res.json({nodes:store.listNodes()}));

app.post("/api/nodes/:id/fail",(req,res)=>{
  const n=store.setNodeState(req.params.id,{online:false});
  if(!n) return res.status(404).json({error:"Node not found"});
  res.json({message:"Node failed",node:n});
});

app.post("/api/nodes/:id/recover",(req,res)=>{
  const n=store.setNodeState(req.params.id,{online:true,partitioned:false});
  if(!n) return res.status(404).json({error:"Node not found"});
  res.json({message:"Node recovered",node:n});
});

app.post("/api/nodes/:id/partition",(req,res)=>{
  const n=store.setNodeState(req.params.id,{partitioned:true});
  if(!n) return res.status(404).json({error:"Node not found"});
  res.json({message:"Network partition simulated",node:n});
});

app.post("/api/nodes/:id/heal-partition",(req,res)=>{
  const n=store.setNodeState(req.params.id,{partitioned:false});
  if(!n) return res.status(404).json({error:"Node not found"});
  res.json({message:"Network partition healed",node:n});
});

app.post("/api/nodes/:id/corrupt/:objectId/:chunkIndex",(req,res)=>{
  const result=store.corruptChunk(req.params.id,req.params.objectId,req.params.chunkIndex);
  if(result.error) return res.status(404).json(result);
  res.json(result);
});

app.get("/api/upload-status",(req,res)=>res.json({
  uploadReady:true,
  maxUploadBytes:maxUploadBytes,
  maxUploadMB:Number((maxUploadBytes/1024/1024).toFixed(1)),
  healthyNodes:store.healthyNodes().length
}));

app.post("/api/objects",upload.single("file"),async(req,res)=>{
  const release=await uploadLimiter.acquire();
  try {
    if(!req.file) return res.status(400).json({error:"file is required"});
    const requestedRf=Number(req.body.replicationFactor || replicationFactor);
    if(!Number.isInteger(requestedRf) || requestedRf < 1) return res.status(400).json({error:"replicationFactor must be a positive integer"});
    const rf=Math.min(requestedRf, Math.max(1, store.healthyNodes().length));
    const objectId=req.body.objectId || crypto.randomUUID();
    const chunks=splitBuffer(req.file.buffer);
    const manifest=await store.putObject({
      objectId,
      originalName:req.file.originalname,
      mimeType:req.file.mimetype,
      chunks,
      rf
    });
    res.status(201).json({
      message:"Object chunked, replicated and stored",
      object:manifest
    });
  } catch(e) {
    const status=/required|replicationFactor|Invalid file|capacity|healthy node/i.test(e.message||"") ? 400 : 503;
    res.status(status).json({error:e.message || "Upload failed"});
  } finally {
    release();
  }
});

app.get("/api/objects",(req,res)=>{
  res.json({
    objects:store.allObjects().map(o=>({
      ...o,
      chunks:o.chunks.map(c=>({...c, buffer:undefined}))
    }))
  });
});

app.get("/api/objects/:id/metadata",(req,res)=>{
  const o=store.getObject(req.params.id);
  if(!o) return res.status(404).json({error:"Object not found"});
  res.json(o);
});

app.get("/api/objects/:id",(req,res)=>{
  const o=store.getObject(req.params.id);
  if(!o) return res.status(404).json({error:"Object not found"});

  const buffers=new Array(o.chunkCount);
  const sourceNodes=[];
  for(const c of o.chunks) {
    const valid=store.getReadableChunk(o,c);
    if(!valid.length) {
      return res.status(503).json({
        error:`Chunk ${c.index} has no valid reachable replica`,
        objectId:o.objectId,
        chunkIndex:c.index,
        hint:"Recover/repair a node and retry."
      });
    }
    buffers[c.index]=valid[0].buffer;
    sourceNodes.push({chunkIndex:c.index,nodeId:valid[0].nodeId});
  }

  res.setHeader("Content-Type",o.mimeType || "application/octet-stream");
  res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(o.originalName || o.objectId)}`);
  res.setHeader("Content-Length",o.size);
  res.setHeader("X-Vault-Object-Id",o.objectId);
  res.setHeader("X-Vault-Object-Checksum",o.objectChecksum);
  res.setHeader("X-Vault-Chunk-Sources",JSON.stringify(sourceNodes));
  res.send(Buffer.concat(buffers));
});

app.delete("/api/objects/:id",(req,res)=>{
  const ok=store.deleteObject(req.params.id);
  if(!ok) return res.status(404).json({error:"Object not found"});
  res.json({success:true,message:"Object deleted from all replicas"});
});

app.get("/api/integrity/summary",(req,res)=>{
  const objects=store.allObjects();
  let objectsHealthy=0, chunksChecked=0, chunksHealthy=0, replicasChecked=0, replicasHealthy=0;
  const issues=[];
  for(const o of objects){
    const result=store.verifyObject(o.objectId);
    if(result.healthy) objectsHealthy++;
    for(const c of (result.chunks||[])){
      chunksChecked++;
      if(c.healthy) chunksHealthy++;
      for(const r of (c.replicas||[])){
        replicasChecked++;
        if(r.healthy) replicasHealthy++;
        if(!r.healthy) issues.push({objectId:o.objectId,index:c.index,nodeId:r.nodeId,error:r.error||"checksum mismatch"});
      }
    }
  }
  const pct=(a,b)=>b?Math.round((a/b)*10000)/100:100;
  res.json({
    objects:{total:objects.length,healthy:objectsHealthy,unhealthy:objects.length-objectsHealthy,percent:pct(objectsHealthy,objects.length)},
    chunks:{checked:chunksChecked,healthy:chunksHealthy,unhealthy:chunksChecked-chunksHealthy,percent:pct(chunksHealthy,chunksChecked)},
    replicas:{checked:replicasChecked,healthy:replicasHealthy,unhealthy:replicasChecked-replicasHealthy,percent:pct(replicasHealthy,replicasChecked)},
    issues:issues.slice(0,100)
  });
});

app.post("/api/verify",(req,res)=>{
  const ids=req.body?.objectIds || store.allObjects().map(o=>o.objectId);
  const results=ids.map(store.verifyObject);
  res.json({
    checked:results.length,
    healthy:results.filter(r=>r.healthy).length,
    unhealthy:results.filter(r=>!r.healthy).length,
    results
  });
});

app.post("/api/repair",async(req,res)=>{
  const results=await store.repairAll(req.body?.objectId);
  res.json({
    repaired:results.filter(r=>r.repaired).length,
    results
  });
});

app.post("/api/rebalance",async(req,res)=>{
  const results=await store.rebalanceAll();
  res.json({
    message:"Rebalance placement pass completed",
    nodeUtilization:store.listNodes(),
    moved:results.filter(r=>r.moved).length,
    results
  });
});

app.get("/api/objects/:id/verify",(req,res)=>{
  const result=store.verifyObject(req.params.id);
  if(result.reason==="metadata not found") return res.status(404).json(result);
  res.json(result);
});

setInterval(async()=>{
  try {
    // Self-healing background pass.
    for(const o of store.allObjects()) {
      const check=store.verifyObject(o.objectId);
      if(!check.healthy || check.chunks.some(c=>c.replicas.filter(r=>r.healthy).length < o.replicationFactor)) {
        await store.repairAll(o.objectId);
      }
    }
  } catch(e) {
    console.error("background repair:",e.message);
  }
}, repairIntervalMs);

setInterval(async()=>{
  try {
    await store.repairAll();
    await store.rebalanceAll();
  } catch(e) { console.error("background rebalance:",e.message); }
}, rebalanceIntervalMs);

app.use((err,req,res,next)=>{
  console.error("[Vault error]", err);
  if(err && err.code === "LIMIT_FILE_SIZE"){
    return res.status(413).json({error:"File is too large for the configured upload limit."});
  }
  if(err && err.code === "LIMIT_UNEXPECTED_FILE"){
    return res.status(400).json({error:"Unexpected upload field. Please select a file using the Upload Object control."});
  }
  res.status(500).json({error:err?.message || "Internal server error"});
});

app.listen(port,()=>{
  console.log(`Vault v2 running at http://localhost:${port}`);
  console.log(`Replication factor: ${replicationFactor}`);
  console.log(`Chunk size: ${chunkSize} bytes`);
  console.log(`Upload concurrency limit: ${maxConcurrentUploads}`);
});