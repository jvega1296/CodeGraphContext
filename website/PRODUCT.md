# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Javier (the fork's maintainer) running this locally via `cgc visualize` to explore
a cross-repo code graph (currently ~60+ Java/Spring microservices plus Terraform
infrastructure) that CodeGraphContext's Python CLI has indexed into an embedded
graph database. He is a senior engineer, comfortable with dense technical UI, and
is the only user of the `/explore` surface in this mode — there is no public
audience for this surface. (Scope note: this website also serves a separate public
marketing site and a standalone browser-only demo for arbitrary visitors; that
experience is out of scope for this redesign, see Positioning below.)

## Product Purpose

The `/explore` page (served locally by the CGC Python backend at
`http://127.0.0.1:8000/explore?backend=...`) is the one surface in this redesign's
scope: a graph-exploration tool for understanding a real, already-indexed
codebase — what calls what, which services talk over which Pub/Sub topics, which
Terraform module backs a given topic, and what the blast radius of changing one
function looks like. Success is a developer finding the answer to a structural
question (a dependency, a caller, an endpoint, a cross-service wire-coupling)
quickly, at the scale of a real multi-repo system, without fighting the canvas.

## Positioning

Unlike a generic graph-database browser (Neo4j Browser/Bloom), this tool's graph
already knows what a codebase is: Function/Class/File nodes from static analysis,
plus Topic/Subscription/Endpoint nodes and PRODUCES_TO/CONSUMES_FROM/SERVES/
INVOKES/BACKED_BY wire-coupling edges connecting real microservices to the real
GCP Pub/Sub and Terraform infrastructure they run on — and a risk-scoring engine
(`analyze_impact`) that already computes blast radius across repos. The current
`/explore` implementation does not yet make most of that visible or fast.

## Operating Context

- Launched locally via `cgc visualize`; the page talks to a same-origin FastAPI
  backend (`src/codegraphcontext/viz/server.py`) over `GET /api/graph`,
  `POST /api/ai_query`, `GET /api/file`. No public network exposure (CORS is
  locked to localhost origins).
- The backend already has a working natural-language-to-Cypher chat pipeline
  (`/api/ai_query`, `call_llm()`), but only wired to Gemini/OpenAI/Atlas
  Cloud/Cheaper Inference/API Route — never Anthropic — and the page's visible
  "chat" affordance is actually an unrelated feature: a Supabase Realtime bridge
  that tells the visitor to open a separate public custom ChatGPT GPT in another
  tab and paste a session token. That bridge exists for the public standalone
  demo (no backend) and is out of place when a real backend is already running.
- The renderer is `react-force-graph-2d`/`-3d` (d3-force on the main thread).
  `graphology` + `graphology-layout-forceatlas2` are already installed
  dependencies but appear unused in this surface.
- Large graphs (hundreds-to-low-thousands of nodes, cross-repo) are the normal
  case for this user, not an edge case.

## Capabilities and Constraints

- Keep the existing query-param contract (`backend`, `repo_path`, `cypher_query`,
  `bundle_url`) that `cgc visualize` and other launch paths rely on — this is the
  interface between the Python CLI and this page, and other code constructs these
  URLs today.
- Keep `GET /api/graph` and `POST /api/ai_query`'s existing response shapes
  working for any caller that already depends on them; extending `call_llm()`
  with a new provider branch must not change the shapes other providers already
  return.
- No new external network service: any LLM call this surface makes must be
  Anthropic Claude via Vertex AI, authenticated the same way the user's other
  Vertex/Claude tooling already authenticates (Google Application Default
  Credentials on the machine running the backend) — not a new proxy service, not
  any other provider, and not the public ChatGPT bridge.
- The standalone, no-backend, parse-in-browser demo mode (`LocalUploader`,
  client-side tree-sitter parsing, the public "Star on GitHub" CTA, GitHub/GitLab
  PAT auth) exists for anonymous public visitors to this site and is a real,
  separate product the fork still carries; this redesign removes that mode's
  chrome only from the backend-connected `/explore` experience, not the
  underlying code paths other routes may still depend on, unless confirmed dead.

## Evidence on Hand

- Real indexed graphs to test against: the `oms-acceptance-test` named CGC
  context already has 4 real cross-repo microservices plus a Terraform module
  indexed with wire-coupling data (Topic/Subscription/Endpoint nodes,
  PRODUCES_TO/CONSUMES_FROM/SERVES/INVOKES edges) from prior work on this fork.
- `analyze_impact(target, direction, max_wire_hops)` already returns a
  `risk_score` per result and a merged cross-repo/cross-infrastructure node set;
  the current `/explore` UI only partially surfaces this (an existing
  `impactTarget`-driven flow in `CodeGraphViewer.tsx` is incomplete).

## Product Principles

1. This is a dense technical tool for one expert user, not a marketing surface:
   favor scanability, keyboard/mouse efficiency, and information density over
   expressive branding.
2. The graph canvas must stay legible and responsive at real multi-repo scale;
   a redesign that looks better but is still slow at 1,000+ nodes has not fixed
   the thing that is actually broken.
3. Finish what is half-built (the existing impact/focus flow, the existing
   `/api/ai_query` pipeline) before inventing new mechanisms — this surface
   already has more working plumbing than its UI currently exposes.
4. No feature that depends on a network service other than this same local
   backend and Google Vertex AI.
