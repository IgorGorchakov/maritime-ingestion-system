# Phase 0: Hygiene & Build Foundation

> Part of the senior-MVP roadmap in `PLAN.md`. Sequence: **0** → 1 → … → 8.
> Status: ☐ Not started

## Objective
Establish baseline engineering hygiene and reproducible builds before any feature work.

## JD competency proven
Architecture / code quality / automation — first-60-seconds reviewer credibility.

## Why (rationale to internalize)
A reviewer judges a repo in seconds. Reproducible builds (Maven wrapper), a one-command runner, ignored junk files, and green CI signal seniority *before* a single line of business code is read.

## Steps
1. `git init` (repo is not yet under version control) and make an initial commit of current state.
2. Add `.gitignore` (target/, *.class, .DS_Store, .idea/, *.iml, logs, Spark/derby warehouse dirs).
3. Add Maven wrapper: `.mvn/wrapper/`, `mvnw`, `mvnw.cmd` (`mvn -N wrapper:wrapper`).
4. Add `Makefile` targets: `up` (docker compose up -d), `down`, `build` (./mvnw clean install -DskipTests), `test` (./mvnw verify), `run-<service>`.
5. Add `.github/workflows/ci.yml`: JDK 17 + cache, `./mvnw -B verify`.
6. Remove committed `.DS_Store`.

## Deliverables
- `.gitignore`, `mvnw`/`.mvn/`, `Makefile`, `.github/workflows/ci.yml`.
- Repo under git with a clean initial commit.

## Verification
- Fresh `./mvnw clean verify` is green.
- `make up` starts infra; `make build` compiles all modules.
- CI workflow passes on push.
