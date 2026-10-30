import { useState, useEffect, useRef, lazy, Suspense } from "react";
import { useSearchParams, useParams, useLocation } from "react-router-dom";
const CodeGraphViewer = lazy(() => import("../components/CodeGraphViewer"));
import LocalUploader from "../components/LocalUploader";
import { motion, AnimatePresence } from "framer-motion";
import { Loader2 } from "lucide-react";
import { toast } from "sonner";
import JSZip from "jszip";
import { parseFilesIntoGraph } from "../lib/parser";
import {
  RepoRef,
  getAuthTokenKey,
  getCacheKey,
  getLegacyCacheKey,
  repoRefFromRoute,
} from "../lib/repo-provider";
import { fetchRepositoryFiles } from "../lib/repo-fetcher";


const IGNORED_DIRS = new Set([
  'node_modules', '.git', '.github', 'dist', 'build', 'out', 'coverage', 
  '.next', '.nuxt', '__pycache__', 'venv', '.venv', 'env', '.env', '.tox',
  'eggs', 'target', '.gradle', '.idea', 'cmake-build-debug', 'bin', 'obj',
  'packages', 'vendor', 'Pods', '.build', 'DerivedData', '.dart_tool',
  '.vscode'
]);

const isPathIgnored = (path: string) => {
  const parts = path.split(/[\/\\]/);
  return parts.some(part => IGNORED_DIRS.has(part));
};

const sanitizePath = (pathStr: string, repoName?: string): string => {
  if (!pathStr) return '';
  
  // Normalize Windows slashes
  let p = pathStr.replace(/\\/g, '/');
  
  // If it's already relative, just return it
  if (p.startsWith('.') || (!p.startsWith('/') && !p.match(/^[a-zA-Z]:\//))) {
    return p.startsWith('./') ? p : './' + p;
  }
  
  // Detect if we can make it relative using the repoName
  if (repoName) {
    const parts = p.split('/');
    const repoIndex = parts.lastIndexOf(repoName);
    if (repoIndex !== -1) {
      return './' + parts.slice(repoIndex).join('/');
    }
  }
  
  // Generic cleanup for absolute paths
  const segments = p.split('/').filter(Boolean);
  if (segments.length > 3) {
    if (p.startsWith('/home/') || p.startsWith('/Users/') || p.includes('/runner/work/')) {
      return './' + segments.slice(-3).join('/');
    }
  }
  
  return p;
};

// ============================================================================
// INDEXEDDB GRAPH CACHE SERVICE
// ============================================================================
const DB_NAME = "cgc-visualizer-cache";
const DB_VERSION = 1;
const STORE_NAME = "graphs";

const openDB = (): Promise<IDBDatabase> => {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "key" });
      }
    };
  });
};

const getCachedGraph = async (ref: RepoRef): Promise<any | null> => {
  try {
    const db = await openDB();
    const cacheKeys = [getCacheKey(ref), getLegacyCacheKey(ref)];
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(STORE_NAME, "readonly");
      const store = transaction.objectStore(STORE_NAME);
      let pending = cacheKeys.length;
      let found: any | null = null;

      for (const key of cacheKeys) {
        const request = store.get(key);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => {
          const result = request.result;
          if (
            !found &&
            result &&
            Date.now() - result.timestamp < 7 * 24 * 60 * 60 * 1000
          ) {
            found = result.graphData;
          }
          pending -= 1;
          if (pending === 0) resolve(found);
        };
      }
    });
  } catch (e) {
    console.error("Failed to read from IndexedDB", e);
    return null;
  }
};

const cacheGraph = async (ref: RepoRef, graphData: any): Promise<void> => {
  try {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(STORE_NAME, "readwrite");
      const store = transaction.objectStore(STORE_NAME);
      const request = store.put({
        key: getCacheKey(ref),
        graphData,
        timestamp: Date.now()
      });
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve();
    });
  } catch (e) {
    console.error("Failed to write to IndexedDB", e);
  }
};

// ============================================================================
// FETCH WITH PROGRESS TRACKING
// ============================================================================
const fetchWithProgress = async (
  url: string,
  onProgress: (loaded: number, total: number) => void
): Promise<Response> => {
  const response = await fetch(url);
  if (!response.ok || !response.body) return response;

  const contentLength = response.headers.get("content-length");
  const total = contentLength ? parseInt(contentLength, 10) : 0;

  let loaded = 0;
  const reader = response.body.getReader();
  const stream = new ReadableStream({
    async start(controller) {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            controller.close();
            break;
          }
          loaded += value.byteLength;
          onProgress(loaded, total);
          controller.enqueue(value);
        }
      } catch (err) {
        controller.error(err);
      }
    }
  });

  return new Response(stream, {
    headers: response.headers,
    status: response.status,
    statusText: response.statusText
  });
};

const Explore = () => {
  const [searchParams] = useSearchParams();
  const location = useLocation();
  const { owner, repo, "*": gitlabSplat } = useParams();
  const isGitlabRoute = location.pathname.startsWith("/gitlab/");
  const repoRef = isGitlabRoute
    ? repoRefFromRoute("gitlab", undefined, undefined, gitlabSplat)
    : repoRefFromRoute("github", owner, repo);
  const [graphData, setGraphData] = useState<any>(null);
  const graphDataRef = useRef<any>(null);
  useEffect(() => {
    graphDataRef.current = graphData;
  }, [graphData]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progressText, setProgressText] = useState("");
  const [progressValue, setProgressValue] = useState(0);
  const [workerLogs, setWorkerLogs] = useState<string[]>([]);

  // Connection parameters for "Playground" mode (CLI/Database)
  const backend = searchParams.get("backend") || "";
  const repoPath = searchParams.get("repo_path") || "";
  const cypherQuery = searchParams.get("cypher_query") || "";
  const bundleUrl = searchParams.get("bundle_url") || "";
  
  // Helper to fetch files using a sequential pool of robust CORS proxies
  const fetchWithFallbackProxies = async (
    url: string,
    onProgress?: (loaded: number, total: number) => void
  ): Promise<Response> => {
    if (!url) throw new Error("URL is empty");
    
    // Try direct fetch first (essential for localhost, relative URLs, or CORS-enabled endpoints)
    try {
      const res = onProgress ? await fetchWithProgress(url, onProgress) : await fetch(url);
      if (res.ok) return res;
    } catch (e) {
      console.warn("Direct fetch failed, falling back to CORS proxies...", e);
    }
    
    // 1. Check raw githubusercontent.com to bypass proxy using jsDelivr CDN directly
    if (url.includes("raw.githubusercontent.com")) {
      const match = url.match(/raw\.githubusercontent\.com\/([^\/]+)\/([^\/]+)\/([^\/]+)\/(.+)$/);
      if (match) {
        const [_, ownerName, repoName, branch, filepath] = match;
        const jsdelivrUrl = `https://cdn.jsdelivr.net/gh/${ownerName}/${repoName}@${branch}/${filepath}`;
        try {
          const res = onProgress ? await fetchWithProgress(jsdelivrUrl, onProgress) : await fetch(jsdelivrUrl);
          if (res.ok) return res;
        } catch (e) {
          console.warn("jsDelivr CDN fetch failed, falling back to CORS proxies...", e);
        }
      }
    }

    const proxies = [
      // Proxy 1: corsproxy.io (Very fast, prefix-based)
      {
        url: (u: string) => `https://corsproxy.io/?${encodeURIComponent(u)}`,
        type: 'direct' as const
      },
      // Proxy 2: CodeTabs (Fast and supports redirects well)
      {
        url: (u: string) => `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(u)}`,
        type: 'direct' as const
      },
      // Proxy 3: allorigins.win JSON endpoint (Handles S3 redirects perfectly via server-side base64 encoding!)
      {
        url: (u: string) => `https://api.allorigins.win/get?url=${encodeURIComponent(u)}`,
        type: 'json-base64' as const
      },
      // Proxy 4: allorigins.win raw (Alternative fallback)
      {
        url: (u: string) => `https://api.allorigins.win/raw?url=${encodeURIComponent(u)}`,
        type: 'direct' as const
      },
      // Proxy 5: thingproxy.freeboard.io (Fallback proxy)
      {
        url: (u: string) => `https://thingproxy.freeboard.io/fetch/${u}`,
        type: 'direct' as const
      }
    ];

    let lastError: any = null;
    for (const proxy of proxies) {
      try {
        const proxiedUrl = proxy.url(url);
        console.log(`[Proxy] Attempting fetch: ${proxiedUrl}`);
        
        if (proxy.type === 'json-base64') {
          // JSON-Base64 Proxy logic (perfect for binary files & redirects)
          const res = await fetch(proxiedUrl);
          if (!res.ok) {
            console.warn(`[Proxy] Status ${res.status} received. Trying next proxy...`);
            continue;
          }
          const json = await res.json();
          if (!json || !json.contents) {
            throw new Error("No contents returned from JSON CORS proxy.");
          }
          
          const base64Data = json.contents;
          const commaIndex = base64Data.indexOf(',');
          const base64String = commaIndex !== -1 ? base64Data.substring(commaIndex + 1) : base64Data;
          
          const binaryString = atob(base64String);
          const len = binaryString.length;
          const bytes = new Uint8Array(len);
          for (let i = 0; i < len; i++) {
            bytes[i] = binaryString.charCodeAt(i);
          }
          
          if (onProgress) {
            onProgress(len, len); // trigger full complete progress
          }
          
          return new Response(bytes.buffer, {
            status: 200,
            headers: { 'Content-Type': 'application/octet-stream' }
          });
        } else {
          // Direct Proxy logic
          const res = onProgress ? await fetchWithProgress(proxiedUrl, onProgress) : await fetch(proxiedUrl);
          if (res.ok) return res;
          if (res.status === 403 || res.status === 429) {
            console.warn(`[Proxy] Status ${res.status} received. Trying next proxy...`);
            continue;
          }
          return res;
        }
      } catch (err) {
        lastError = err;
        console.warn("[Proxy] Connection failed, trying next fallback proxy...", err);
      }
    }
    throw lastError || new Error("Failed to fetch via all available CORS proxies.");
  };

  // If route parameters resolve to a repository, auto-fetch and index the codebase.
  useEffect(() => {
    if (!repoRef) return;

    if (!isGitlabRoute && owner?.toLowerCase() === "explore") return;

    const autoFetchAndIndex = async () => {
      setLoading(true);
      setError(null);
      
      try {
        try {
          const cached = await getCachedGraph(repoRef);
          if (cached) {
            setProgressText("Loading cached codebase graph...");
            setProgressValue(90);
            await new Promise((r) => setTimeout(r, 100));
            setGraphData(cached);
            setProgressText("Complete!");
            setProgressValue(100);
            setLoading(false);
            console.log(`[Cache] Loaded repository graph for ${repoRef.fullPath} from IndexedDB cache.`);
            return;
          }
        } catch (cacheErr) {
          console.warn("[Cache] Error reading from cache:", cacheErr);
        }

        const { files, fileContents, latestCommitSha } = await fetchRepositoryFiles(repoRef, {
          onProgressText: setProgressText,
          onProgressValue: setProgressValue,
          isPathIgnored,
          fetchWithProgress,
          fetchWithFallbackProxies,
        });

        // --- COMMON SEMANTIC INDEXING PHASE ---
        setProgressText("Initializing WebAssembly semantic engine...");
        setProgressValue(60);
        
        // Load custom indexer settings from localStorage
        let localConfig = { indexVariables: false, maxNodes: 100000, maxEdges: 50000 };
        try {
          const saved = localStorage.getItem('cgc_indexer_config');
          if (saved) {
            localConfig = { ...localConfig, ...JSON.parse(saved) };
          }
        } catch (e) {}

        const graphData = await parseFilesIntoGraph(
          files,
          (msg, val, diagLog) => {
            if (diagLog) {
              setWorkerLogs(prev => [...prev, diagLog]);
            } else {
              setProgressText(msg);
              setProgressValue(val);
            }
          },
          { 
            indexVariables: localConfig.indexVariables,
            maxNodes: localConfig.maxNodes,
            maxEdges: localConfig.maxEdges
          }
        );
        
        setProgressText("Complete!");
        setProgressValue(100);
        await new Promise((r) => setTimeout(r, 450));
        
        const finalGraphData = {
          ...graphData,
          fileContents,
          metadata: {
            repo: repoRef.fullPath,
            provider: repoRef.provider,
            version: latestCommitSha ? (latestCommitSha.length === 40 && /^[0-9a-fA-F]+$/.test(latestCommitSha) ? latestCommitSha.substring(0, 7) : latestCommitSha) : "1.0.0",
            commit: latestCommitSha || "",
            exported_at: new Date().toISOString(),
            generator: "WASMv0.0.1"
          }
        };
        setGraphData(finalGraphData);
        
        try {
          await cacheGraph(repoRef, finalGraphData);
          console.log(`[Cache] Successfully cached repository graph for ${repoRef.fullPath} in IndexedDB.`);
        } catch (cacheErr) {
          console.warn("[Cache] Failed to save graph to IndexedDB:", cacheErr);
        }
      } catch (err: any) {
        console.error("Auto-Index Error:", err);
        setError(err.message);
        toast.error("Auto-Indexing failed: " + err.message);
      } finally {
        setLoading(false);
      }
    };

    autoFetchAndIndex();
  }, [repoRef?.fullPath, repoRef?.provider, owner, repo, isGitlabRoute, gitlabSplat]);

  // ✅ Automatic Graph Caching: Safely caches all graphs (local folder uploads, ZIPs, CGC bundles) to IndexedDB!
  useEffect(() => {
    if (!graphData) return;

    const saveToCache = async () => {
      let cacheRef = repoRef;
      const metaRepo = graphData.metadata?.repo || "";

      if (!cacheRef && metaRepo.includes("/")) {
        const parts = metaRepo.split("/").filter(Boolean);
        const repoName = parts[parts.length - 1];
        const ownerPath = parts.slice(0, -1).join("/");
        const provider = graphData.metadata?.provider === "gitlab" ? "gitlab" : "github";
        cacheRef = {
          provider,
          owner: ownerPath,
          repo: repoName,
          fullPath: metaRepo,
          host: provider === "gitlab" ? "gitlab.com" : "github.com",
        };
      }

      if (!cacheRef) return;

      try {
        await cacheGraph(cacheRef, graphData);
        console.log(`[Cache] Automatically cached active graph for ${cacheRef.fullPath} in IndexedDB.`);
      } catch (cacheErr) {
        console.warn("[Cache] Failed to auto-save graph to IndexedDB:", cacheErr);
      }
    };

    saveToCache();
  }, [graphData, repoRef]);

  
  // If bundleUrl is present, we download and parse it client-side
  useEffect(() => {
    if (!bundleUrl) return;

    const fetchBundle = async () => {
      setLoading(true);
      setError(null);
      try {
        const isBase64 = bundleUrl.endsWith('.base64') || bundleUrl.endsWith('.txt');
        setProgressText(isBase64 ? "Downloading and decoding bundle..." : "Downloading bundle...");
        setProgressValue(5);
        
        const response = await fetchWithFallbackProxies(bundleUrl, (loaded, total) => {
          const mbLoaded = (loaded / 1024 / 1024).toFixed(2);
          if (total > 0) {
            const pct = Math.round((loaded / total) * 100);
            setProgressText(`Downloading bundle: ${pct}% (${mbLoaded} MB)`);
            setProgressValue(5 + Math.floor(pct * 0.45)); // maps 0-100% to 5-50% progress bar
          } else {
            setProgressText(`Downloading bundle: ${mbLoaded} MB...`);
          }
        });
        
        if (!response.ok) {
          throw new Error(`Failed to fetch bundle from URL (${response.status})`);
        }
        
        setProgressText(isBase64 ? "Decoding bundle data..." : "Unzipping bundle in-memory...");
        setProgressValue(55);
        
        let buffer: ArrayBuffer;
        if (isBase64) {
          const base64Text = await response.text();
          const binaryString = atob(base64Text.trim());
          const len = binaryString.length;
          const bytes = new Uint8Array(len);
          for (let i = 0; i < len; i++) {
            bytes[i] = binaryString.charCodeAt(i);
          }
          buffer = bytes.buffer;
        } else {
          buffer = await response.arrayBuffer();
        }
        
        setProgressText("Unzipping bundle in-memory...");
        setProgressValue(60);
        const jszip = await JSZip.loadAsync(buffer);
        
        const nodesFile = jszip.file("nodes.jsonl");
        const edgesFile = jszip.file("edges.jsonl");
        
        if (!nodesFile || !edgesFile) {
          throw new Error("Invalid CGC bundle: nodes.jsonl and edges.jsonl are required.");
        }
        
        let metadata: any = {};
        if (jszip.file("metadata.json")) {
          const metaText = await jszip.file("metadata.json")!.async("text");
          try {
            metadata = JSON.parse(metaText);
          } catch (e) {
            console.warn("Could not parse metadata.json", e);
          }
        }
        
        const repoName = metadata.repo || "";
        
        const nodesText = await nodesFile.async("text");
        const nodeLines = nodesText.split("\n").filter(line => line.trim() !== "");
        const nodes = nodeLines.map((line, idx) => {
          try {
            const nodeData = JSON.parse(line);
            const labels = nodeData._labels || [];
            const id = nodeData._id;
            
            // Extract properties
            const properties: Record<string, any> = {};
            for (const key of Object.keys(nodeData)) {
              if (key !== '_labels' && key !== '_id') {
                properties[key] = nodeData[key];
              }
            }
            
            // Clean absolute paths in node properties
            for (const key of Object.keys(properties)) {
              if (typeof properties[key] === 'string') {
                const val = properties[key];
                if (val.startsWith('/') || val.match(/^[a-zA-Z]:\\/) || val.includes('\\') || val.includes('/')) {
                  if (key === 'path' || key === 'file' || key === 'repo_path' || key === 'import_path') {
                    properties[key] = sanitizePath(val, repoName);
                  }
                }
              }
            }
            
            let displayName = String(properties.name || properties.label || properties.path || 'Unknown');
            if (displayName.startsWith('/') || displayName.includes('\\') || displayName.includes('/')) {
              displayName = sanitizePath(displayName, repoName);
            }
            
            const type = labels[0] ? (labels[0].charAt(0).toUpperCase() + labels[0].slice(1)) : 'Other';
            
            return {
              id: String(id),
              name: displayName,
              label: displayName,
              type: type,
              file: String(properties.path || properties.file || ''),
              val: (labels.length > 0 && ['Repository', 'Class', 'Interface', 'Trait'].includes(labels[0])) ? 4 : 2,
              properties: properties
            };
          } catch (err) {
            console.error("Failed to parse node line at index", idx, err);
            return null;
          }
        }).filter(Boolean);
        
        const edgesText = await edgesFile.async("text");
        const edgeLines = edgesText.split("\n").filter(line => line.trim() !== "");
        const links = edgeLines.map((line, idx) => {
          try {
            const edgeData = JSON.parse(line);
            return {
              id: `${edgeData.from}_to_${edgeData.to}_${edgeData.type}_${idx}`,
              source: String(edgeData.from),
              target: String(edgeData.to),
              type: String(edgeData.type).toUpperCase()
            };
          } catch (err) {
            console.error("Failed to parse edge line at index", idx, err);
            return null;
          }
        }).filter(Boolean);
        
        const filePaths: string[] = [];
        for (const n of nodes as any[]) {
          if (n.file && n.type.toLowerCase() === 'file') {
            filePaths.push(n.file);
          }
        }
        const sortedFiles = Array.from(new Set(filePaths)).sort();
        
        setGraphData({
          nodes,
          links,
          files: sortedFiles,
          fileContents: {},
          metadata
        });
      } catch (err: any) {
        console.error("Fetch Bundle Error:", err);
        setError(err.message);
        toast.error("Failed to load bundle: " + err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchBundle();
  }, [bundleUrl]);

  // If backend param is present, we automatically fetch from the local python server
  useEffect(() => {
    if (!backend && !cypherQuery) return;

    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const url = new URL("/api/graph", backend || window.location.origin);
        if (repoPath) url.searchParams.append("repo_path", repoPath);
        if (cypherQuery) url.searchParams.append("cypher_query", cypherQuery);

        const response = await fetch(url.toString());
        if (!response.ok) {
          const errData = await response.json().catch(() => ({}));
          throw new Error(errData.detail || `Server error (${response.status})`);
        }

        const data = await response.json();
        setGraphData(data);
      } catch (err: any) {
        console.error("Fetch Error:", err);
        setError(err.message);
        toast.error("Failed to connect to local index: " + err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [backend, repoPath, cypherQuery]);

  if (loading) {
    const isAutoIndexing = !!repoRef;
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-black text-center px-6 w-full relative overflow-hidden">
        
        <div className="w-full max-w-md mx-auto flex flex-col items-center justify-center relative z-10">
          <Loader2 className="w-14 h-14 animate-spin text-white mb-6" />
          <p className="text-[10px] font-mono uppercase tracking-widest text-gray-500 mb-4">
            {isAutoIndexing 
              ? progressText 
              : (bundleUrl ? "Downloading and parsing pre-indexed CGC bundle..." : "Connecting to local database...")}
          </p>
          {isAutoIndexing && (
            <div className="w-full bg-white/5 rounded-full h-1.5 mt-2 overflow-hidden border border-white/10">
              <div 
                className="bg-white h-1.5 rounded-full transition-all duration-300 ease-out" 
                style={{ width: `${progressValue}%` }}
              />
            </div>
          )}
          {isAutoIndexing && (
            <p className="text-[10px] text-gray-600 font-mono mt-3 uppercase tracking-widest">{progressValue}%</p>
          )}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-black px-6 text-center text-white">
        <div className="w-full max-w-md p-8 rounded-3xl border border-white/10 bg-black relative">
          <div className="absolute top-0 left-0 right-0 h-0.5 bg-white rounded-t-3xl" />
          
          <h1 className="text-sm font-black uppercase tracking-widest mb-3 text-white">Access or Loading Error</h1>
          <p className="text-[10px] font-mono text-gray-500 uppercase tracking-widest max-w-md mb-6 leading-relaxed">{error}</p>
          
          {repoRef && (
            <div className="mb-6 p-4 rounded-2xl bg-white/5 border border-white/10 text-left">
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-400 block mb-2">
                Private Repository? Access Token (PAT)
              </label>
              <input
                type="password"
                placeholder="PAT — GitHub (ghp_...) or GitLab (glpat-...)"
                defaultValue={localStorage.getItem(getAuthTokenKey(repoRef)) || ""}
                onChange={(e) => {
                  const val = e.target.value.trim();
                  const tokenKey = getAuthTokenKey(repoRef);
                  if (val) {
                    localStorage.setItem(tokenKey, val);
                  } else {
                    localStorage.removeItem(tokenKey);
                  }
                }}
                className="w-full bg-black border border-white/20 rounded-full px-4 py-2.5 text-[10px] font-mono text-white placeholder-gray-600 focus:outline-none focus:border-white transition-colors uppercase tracking-widest"
              />
              <p className="text-[8px] font-mono text-gray-600 mt-2 leading-normal uppercase tracking-widest">
                {repoRef.provider === "gitlab"
                  ? "Private GitLab repos need a Personal Access Token with read_api scope."
                  : "Private GitHub repos need a Personal Access Token with read:repo scope."}
              </p>
            </div>
          )}
          
          <div className="flex gap-3">
            <button
              onClick={() => {
                window.location.href = '/explore';
              }}
              className="w-full bg-transparent hover:bg-white/5 border border-white/20 text-white py-2.5 rounded-full font-black text-[10px] uppercase tracking-widest transition-colors"
            >
              Go to Explore
            </button>
            <button 
              onClick={() => window.location.reload()} 
              className="w-full bg-white text-black hover:bg-gray-200 py-2.5 rounded-full font-black text-[10px] uppercase tracking-widest transition-colors border-0"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <main className="min-h-screen bg-background pt-32 md:pt-36 pb-12 px-6 flex flex-col items-center">
      <AnimatePresence mode="wait">
        {!graphData ? (
          <motion.div 
            key="uploader"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95 }}
            className="w-full max-w-4xl mx-auto flex flex-col items-center mt-12"
          >
            <div className="text-center mb-12">
              <h1 className="text-3xl sm:text-4xl md:text-5xl font-black mb-4 gradient-text uppercase tracking-tight">
                Graph Explorer
              </h1>
              <p className="text-[10px] font-mono text-gray-500 uppercase tracking-widest max-w-2xl mx-auto">
                Instantly visualize your code architecture. Scan local files via WebAssembly or connect to your local CLI index.
              </p>
            </div>
            
            <div className="w-full max-w-2xl">
              <LocalUploader onComplete={setGraphData} />
            </div>
          </motion.div>
        ) : (
          <div className="w-full h-full relative">
            <Suspense fallback={
              <div className="w-full h-full flex flex-col items-center justify-center bg-black text-white font-mono text-sm gap-3">
                <Loader2 className="h-8 w-8 animate-spin text-purple-500" />
                <span>Loading Viewer...</span>
              </div>
            }>
              <CodeGraphViewer 
                key="viewer" 
                data={graphData} 
                onClose={() => setGraphData(null)}
              />
            </Suspense>
          </div>
        )}
      </AnimatePresence>
    </main>
  );
};

export default Explore;
