# Phase 8: Agentic AI Artifacts + Docs

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: 0 → … → 7 → **8** (final).
> Status: ☐ Not started

## Objective
Make the platform agent-operable and self-documenting; capture the architecture for both humans and AI agents.

## JD competency proven
Agentic AI experience (Claude Code, MCP, custom .md files, subagent workflows) — a listed nice-to-have, and this MVP is itself built with Claude Code.

## Why (rationale to internalize)
- `CLAUDE.md` is a machine-readable architecture contract that keeps an agent productive in the repo.
- An **MCP server** exposing the platform as tools turns "a demo" into "an agent-operable system" — an analyst (or agent) can query risk, list dark vessels, and replay the DLQ without bespoke UI.

## Steps
1. `CLAUDE.md`: architecture overview, ports table, `make` targets, conventions (Avro-first, manual-ack, validation-before-enrichment).
2. README rewrite: Lambda-architecture framing, Grafana screenshots, full run instructions.
3. `.mcp/` server exposing tools: `get_vessel_risk`, `query_loitering_events`, `list_dark_vessels`, `replay_dlq`.
4. Document a "maritime-analyst" subagent workflow.

## Deliverables
- `CLAUDE.md`, rewritten `README.md`, MCP server + documented subagent workflow.

## Verification
- `claude mcp` lists the server.
- `get_vessel_risk` returns live data from the running stack.
