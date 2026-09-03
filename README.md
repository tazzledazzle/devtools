# Code Helper

Single-user AI code assistant: explain code, answer questions with web search, suggest edits, generate and refactor code, run tests, and verify output. Uses a **CrewAI** hierarchical crew (Manager → Researcher, Coder, Runner), **hybrid RAG** (persistent project index), and a **self-hosted LLM** in Kubernetes.

**Components:** Crew API (FastAPI + CrewAI), Runner service, Ingest pipeline (K8s Job), Vector DB (Chroma), Chat UI, CLI.

## Services

| Service | Location | Port |
|---------|----------|------|
| Crew API | `crew_api/` | 8000 |
| Runner | `runner/` | 8080 |
| Ingest pipeline | `ingest/` | — (one-shot) |
| CLI | `cli/` | — |
| Chat UI | `chat_ui/` | 3000 (static) |
| Kubernetes manifests | `k8s/` | — |

## Quick Start

**Docker Compose (local):**
```bash
docker compose up -d
curl http://localhost:8000/health   # {"status":"ok"}
```

**CLI:**
```bash
pip install -e .
code-helper chat --path /path/to/project --message "Explain this codebase"
code-helper run-tests --path /path/to/project
```

**Chat UI:**
```bash
cd chat_ui && python3 -m http.server 3000
# Open http://localhost:3000
```

## Documentation

- [DOCKER.md](DOCKER.md) — build images, compose, smoke tests
- [k8s/README.md](k8s/README.md) — Kubernetes deployment
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — run, operate, and troubleshoot
- [docs/CONFIG.md](docs/CONFIG.md) — environment variables
- [docs/STATE.md](docs/STATE.md) — index status and ingest Job lifecycle
- [docs/FAILURE-MODES.md](docs/FAILURE-MODES.md) — per-failure runbooks
- [docs/codebase/ARCHITECTURE.md](docs/codebase/ARCHITECTURE.md) — service architecture and data flow
- [docs/codebase/STACK.md](docs/codebase/STACK.md) — language, frameworks, dependencies
- [docs/codebase/STRUCTURE.md](docs/codebase/STRUCTURE.md) — directory layout and file purposes

## Other Directories

The following directories exist in this repo but contain no implementation:

| Directory | Status |
|-----------|--------|
| `atlas-name-check/` | Empty scaffold |
| `bin-range-coverage/` | Empty scaffold |
| `card-validator/` | Empty scaffold |
| `fraud-detection/` | Empty scaffold |
| `incident-alert-detect/` | Empty scaffold |
| `subscription-notifications/` | Empty scaffold |
| `cmake_to_bazel/` | Empty |
| `codebase-health-monitor/` | Empty |
| `maple-rewrite/` | Empty |
| `observability/` | Empty |
| `personal-copilot/` | Empty |
| `strip-stream/` | Empty |
| `cdk-libs/` | Empty |
