# /security-review

Perform an automated security review of the pull request diff. This command is
invoked by the `claude-security-review` GitHub Action. It can also be run
locally by a developer to preview what the CI reviewer would flag on their
branch.

The security patterns and anti-patterns themselves live in
`@.claude/rules/security.md` — read that file before starting to understand
what counts as a real issue for this codebase. This command covers the
**filtering, scoping, and output rules** the reviewer must follow.

---

## CRITICAL RULE: Maximum 3 comments per PR

Report the three most severe, actionable findings. If you have more than three
candidates, drop the lowest-severity ones. If you have zero genuine findings,
post nothing.

## CRITICAL RULE: Only Critical, High, or Medium severity

Do not report Low-severity issues. If a finding is not at least Medium, do not
comment.

## CRITICAL RULE: Only exploitable issues in production code

Every finding must describe a concrete attack path. "This could be more
secure" is not a finding. "A logged-in user can read another state's data via
X" is a finding.

---

## Scope

### Only review files that are part of the PR diff

Do not comment on files outside the diff. Do not comment on the repository at
large.

### Ignore these categories entirely

These are either handled elsewhere or not exploitable:

- **PII exposure of any kind** — a separate dedicated GitHub Action scans for
  PII logging, PII in error messages, PII in URLs, PII in client storage, and
  missing redaction. Do not duplicate that coverage.
- **SQL injection in backend data processing code** — ETL pipelines, BigQuery
  view builders, ingest views, calculators, Airflow DAGs, Beam pipelines,
  aggregated metrics, validation queries, notebooks. These construct SQL from
  developer-controlled values and are not exploitable. See
  `@.claude/rules/security.md` for the full scoping rules.
- **Test code** (`recidiviz/tests/`, fixtures, mocks) — unless it contains real
  credentials or production data.
- **Development-only scripts** (`recidiviz/tools/`) — unless they access
  production systems.
- **Auto-generated code** — protobuf (`*_pb2.py`), generated clients, Alembic
  migration files unless hand-edited, dependency lock files.
- **Documentation** (`*.md`, `docs/`, READMEs) — unless they contain real
  credentials or API keys.
- **Code style, formatting, naming, readability, performance, test coverage,
  architecture** — none of these are security findings.
- **Type errors, lint failures, import errors** — mypy, pylint, and CI catch
  these. Not your job.
- **Vague guidance** — "consider adding validation", "could be more secure",
  "might want to use...". If you cannot describe an attack path, it is not a
  finding.
- **Theoretical vulnerabilities with no realistic exploit** — issues only
  reachable with direct filesystem or database access, internal-only tools
  with existing access controls, local development scripts.
- **Patterns already documented as accepted trade-offs** in CLAUDE.md or in
  the PR description.

### Do report

Anything that meets all of these:
1. **Exploitable** — a clear attack path exists
2. **High impact** — data breach, auth bypass, privilege escalation, or
   system compromise
3. **Actionable** — specific remediation steps can be given
4. **Novel** — not caught by existing CI checks
5. **In production code** — affects code that runs in production

Severity definitions:
- **Critical** — immediate data breach or system compromise risk (SQL
  injection in a user-facing route, hardcoded production credentials, auth
  bypass)
- **High** — significant security weakness (missing auth on a sensitive
  endpoint, XSS in user-facing app, secrets in logs, webhook without
  signature verification)
- **Medium** — important weakness without an immediate exploit (missing input
  validation on a lightly-exposed endpoint, weak crypto in a non-critical
  path, overly permissive CORS on a staging environment)

---

## Output format

Post findings as **inline PR review comments** on the specific line the issue
occurs on. Use the `mcp__github_inline_comment__create_inline_comment` tool.
Do not post a summary comment. Do not post "LGTM" or "no issues found"
comments.

Each inline comment must follow this structure:

```
**[SEVERITY] Short issue title**

**Issue:** One or two sentences describing the specific vulnerability.

**Impact:** What an attacker could do. Be concrete — "a logged-in user from
state A could read state B's person records via the unscoped query" rather
than "data could be exposed".

**Remediation:** One or two concrete steps.

**Example fix:**
```python
# Corrected code here
```
```

Do not include: generic summaries, "overall assessment" sections, positive
feedback, lists of things that are correct, acknowledgments of human reviewer
comments, or meta-commentary about the review process.

---

## Review procedure

1. Read `@.claude/rules/security.md` to load the security patterns and
   anti-patterns for this codebase.
2. Fetch the PR diff with `gh pr diff <pr_number>`.
3. For each changed file, decide whether it falls inside the review scope
   (production code, user-facing, handles data). Skip files that fall into the
   "ignore" categories above.
4. For in-scope files, read the surrounding context with `gh pr view` and
   direct file reads as needed to understand whether a pattern is actually
   exploitable (not just superficially suspicious).
5. Build a candidate list of findings. For each candidate, confirm:
   - It matches a pattern in `@.claude/rules/security.md`
   - It is exploitable in production
   - It is at least Medium severity
   - It is not already covered by existing CI / the PII action
6. Trim to the top 3 by severity and specificity.
7. Post each finding as an inline review comment on the offending line using
   the format above. If no finding qualifies, post nothing.
