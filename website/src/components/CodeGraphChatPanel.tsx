import { useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Loader2, Send, ChevronDown, ChevronRight, AlertTriangle } from "lucide-react";

export interface CodeGraphChatPanelProps {
  backendUrl: string;
  repoPath: string | null;
  isDark: boolean;
  onGraphResult: (result: any) => void;
}

// Subset markdown → HTML: headers, bold, italic, inline code, code blocks, bullet lists.
// No external library needed for the subset LLMs actually produce for graph explanations.
function renderMarkdown(text: string): string {
  if (!text) return "";
  const lines = text.split("\n");
  const out: string[] = [];
  let inCodeBlock = false;
  let codeLines: string[] = [];
  let listItems: string[] = [];

  function flushList() {
    if (listItems.length === 0) return;
    out.push('<ul class="list-disc list-inside space-y-0.5 my-1">' + listItems.join("") + "</ul>");
    listItems = [];
  }

  function inline(s: string): string {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
      .replace(/\*(.*?)\*/g, "<em>$1</em>")
      .replace(/`([^`]+)`/g, '<code class="text-purple-300 bg-black/30 px-1 rounded text-[10.5px] font-mono">$1</code>');
  }

  function esc(s: string): string {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  for (const line of lines) {
    if (line.startsWith("```")) {
      if (inCodeBlock) {
        out.push(
          '<pre class="overflow-x-auto text-[10.5px] font-mono bg-black/40 border border-white/5 rounded-lg p-2 my-1 whitespace-pre-wrap">' +
            esc(codeLines.join("\n")) +
            "</pre>"
        );
        codeLines = [];
        inCodeBlock = false;
      } else {
        flushList();
        inCodeBlock = true;
      }
      continue;
    }
    if (inCodeBlock) {
      codeLines.push(line);
      continue;
    }

    if (/^### /.test(line)) {
      flushList();
      out.push('<h3 class="text-[12px] font-bold mt-2 mb-0.5">' + inline(line.slice(4)) + "</h3>");
    } else if (/^## /.test(line)) {
      flushList();
      out.push('<h2 class="text-[13px] font-bold mt-3 mb-1">' + inline(line.slice(3)) + "</h2>");
    } else if (/^# /.test(line)) {
      flushList();
      out.push('<h1 class="text-[14px] font-bold mt-3 mb-1">' + inline(line.slice(2)) + "</h1>");
    } else if (/^[*-] /.test(line)) {
      listItems.push('<li class="text-[11.5px] leading-relaxed">' + inline(line.slice(2)) + "</li>");
    } else if (line.trim() === "") {
      flushList();
    } else {
      flushList();
      out.push('<p class="text-[11.5px] leading-relaxed mb-0.5">' + inline(line) + "</p>");
    }
  }
  flushList();
  return out.join("");
}

// The backend 400 "no provider configured" message contains this key env var name.
const NO_PROVIDER_MARKER = "ANTHROPIC_VERTEX_PROJECT_ID";

function isNoProviderError(msg: string): boolean {
  return msg.includes(NO_PROVIDER_MARKER) || msg.includes("AI querying requires");
}

export default function CodeGraphChatPanel({
  backendUrl,
  repoPath,
  isDark,
  onGraphResult,
}: CodeGraphChatPanelProps) {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [cypherExpanded, setCypherExpanded] = useState(false);

  const panelBg = isDark
    ? "bg-[#0d0d0d] border-white/[0.07]"
    : "bg-white border-black/10";
  const inputCls = isDark
    ? "bg-white/5 border border-white/10 text-white placeholder:text-gray-600"
    : "bg-black/5 border border-black/10 text-gray-900 placeholder:text-gray-400";
  const textColor = isDark ? "text-gray-200" : "text-gray-800";
  const mutedCls = isDark ? "text-gray-500" : "text-gray-500";
  const dividerCls = isDark ? "border-white/[0.06]" : "border-black/[0.06]";

  const handleSubmit = useCallback(async () => {
    const trimmed = query.trim();
    if (!trimmed || loading) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setCypherExpanded(false);

    try {
      const url = `${backendUrl.replace(/[/\\]+$/, "")}/api/ai_query`;
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: trimmed, repo_path: repoPath || null }),
      });

      let data: any;
      try {
        data = await response.json();
      } catch {
        data = { detail: `Server error (${response.status})` };
      }

      if (!response.ok) {
        throw new Error(data.detail || `Server error (${response.status})`);
      }

      if (data.success) {
        setResult(data);
        if ((data.nodes && data.nodes.length > 0) || (data.links && data.links.length > 0)) {
          onGraphResult(data);
        }
      } else {
        setError(data.error || "Claude returned an unexpected response.");
      }
    } catch (err: any) {
      setError(err.message || "Failed to reach the backend.");
    } finally {
      setLoading(false);
    }
  }, [query, loading, backendUrl, repoPath, onGraphResult]);

  return (
    <div
      className={`flex flex-col rounded-2xl border shadow-2xl overflow-hidden backdrop-blur-xl ${panelBg}`}
      style={{ maxHeight: 500 }}
    >
      {/* ── Input row ── */}
      <div className="p-3 flex-shrink-0">
        <div className="flex gap-2">
          <textarea
            rows={2}
            className={`flex-1 rounded-xl text-xs p-2.5 resize-none focus:outline-none focus:ring-1 focus:ring-purple-500/50 transition-all ${inputCls}`}
            placeholder="Ask Claude about this graph…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleSubmit();
              }
            }}
          />
          <button
            onClick={handleSubmit}
            disabled={loading || !query.trim()}
            className="self-end flex-shrink-0 w-9 h-9 flex items-center justify-center rounded-xl bg-purple-500 hover:bg-purple-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            title="Ask Claude (Enter)"
          >
            {loading ? (
              <Loader2 className="w-4 h-4 text-white animate-spin" />
            ) : (
              <Send className="w-4 h-4 text-white" />
            )}
          </button>
        </div>
      </div>

      {/* ── Results area ── */}
      <AnimatePresence mode="wait">
        {loading && (
          <motion.div
            key="loading"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className={`px-3 pb-3 flex items-center gap-2 border-t ${dividerCls}`}
          >
            <div className="pt-2 flex items-center gap-2">
              <Loader2 className="w-3.5 h-3.5 text-purple-400 animate-spin flex-shrink-0" />
              <span className={`text-[11px] uppercase tracking-widest font-bold ${mutedCls}`}>
                Claude is thinking…
              </span>
            </div>
          </motion.div>
        )}

        {!loading && error && (
          <motion.div
            key="error"
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }}
            className={`px-3 pb-3 border-t ${dividerCls}`}
          >
            <div className="pt-2">
              {isNoProviderError(error) ? (
                <div className="p-3 rounded-xl bg-amber-500/10 border border-amber-500/20 space-y-1.5">
                  <div className="flex items-center gap-2">
                    <AlertTriangle className="w-3.5 h-3.5 text-amber-400 flex-shrink-0" />
                    <span className="text-[11px] font-bold text-amber-300 uppercase tracking-wide">
                      Claude not configured
                    </span>
                  </div>
                  <p className="text-[11px] text-amber-200/80 leading-relaxed">
                    Set{" "}
                    <code className="font-mono bg-black/30 px-1 rounded">
                      ANTHROPIC_VERTEX_PROJECT_ID
                    </code>{" "}
                    (and optionally{" "}
                    <code className="font-mono bg-black/30 px-1 rounded">CLOUD_ML_REGION</code>) in
                    the environment running the backend, then restart the server.
                  </p>
                </div>
              ) : (
                <div className="p-3 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-[11px] font-mono leading-relaxed">
                  {error}
                </div>
              )}
            </div>
          </motion.div>
        )}

        {!loading && result && (
          <motion.div
            key="result"
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0 }}
            className={`flex-1 overflow-y-auto border-t ${dividerCls} custom-scrollbar`}
            style={{ minHeight: 0 }}
          >
            <div className="px-3 pt-3 pb-3 space-y-3">
              {/* Explanation rendered as markdown */}
              {result.explanation && (
                <div>
                  <div className={`text-[9px] font-black uppercase tracking-widest mb-1.5 ${mutedCls}`}>
                    Claude's Answer
                  </div>
                  <div
                    className={textColor}
                    dangerouslySetInnerHTML={{ __html: renderMarkdown(result.explanation) }}
                  />
                </div>
              )}

              {/* Collapsible Cypher query (collapsed by default) */}
              {result.cypher_query && (
                <div
                  className={`rounded-xl border overflow-hidden ${
                    isDark ? "border-white/[0.07] bg-white/[0.03]" : "border-black/[0.07] bg-black/[0.03]"
                  }`}
                >
                  <button
                    onClick={() => setCypherExpanded((v) => !v)}
                    className={`w-full flex items-center justify-between px-3 py-2 text-left transition-colors ${
                      isDark ? "hover:bg-white/5" : "hover:bg-black/5"
                    }`}
                  >
                    <span className={`text-[9px] font-black uppercase tracking-widest flex items-center gap-1.5 ${mutedCls}`}>
                      Generated Cypher
                    </span>
                    {cypherExpanded ? (
                      <ChevronDown className="w-3 h-3 text-gray-500 flex-shrink-0" />
                    ) : (
                      <ChevronRight className="w-3 h-3 text-gray-500 flex-shrink-0" />
                    )}
                  </button>
                  {cypherExpanded && (
                    <pre className="px-3 pb-3 text-[10px] font-mono text-purple-300 overflow-x-auto whitespace-pre-wrap select-all leading-relaxed">
                      {result.cypher_query}
                    </pre>
                  )}
                  {result.translation_explanation && !cypherExpanded && (
                    <p className={`px-3 pb-2 text-[10px] leading-relaxed ${mutedCls}`}>
                      {result.translation_explanation}
                    </p>
                  )}
                </div>
              )}

              {/* Node count badge when graph was updated */}
              {result.nodes && result.nodes.length > 0 && (
                <div className="flex items-center gap-1.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-purple-400 flex-shrink-0" />
                  <span className="text-[10px] text-purple-400 font-bold uppercase tracking-widest">
                    {result.nodes.length} node{result.nodes.length !== 1 ? "s" : ""} highlighted in graph
                  </span>
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
