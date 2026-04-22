# Security Rules

These rules apply to all code in this repository and are always loaded into
Claude Code sessions. They encode the security patterns and anti-patterns that
Recidiviz requires, derived from our state partner compliance requirements, the
existing codebase conventions, and historical incidents.

**PII handling is out of scope for this file.** A separate dedicated GitHub
Action scans for PII exposure (logs, error messages, URLs, client-side storage).
Do not duplicate that coverage here.

---

## State-Specific Data Isolation

We serve multiple state partners. Data must not leak between states under any
circumstance. This is the single most important security property of the
codebase.

- Queries that read state-partitioned data must be scoped to the authenticated
  user's `state_code` or to an explicit developer-controlled `StateCode` value.
- Never use `SELECT *` without a `state_code` filter against state-partitioned
  tables.
- Never accept a `state_code` from an HTTP request parameter without validating
  it against the authenticated user's authorized states.
- Never share credentials or service accounts across state integrations.

**Bad — user-supplied state code with no validation, direct string
interpolation:**
```python
state_code = request.args.get('state_code')
query = f"SELECT * FROM state_person WHERE state_code = '{state_code}'"
```

**Good — validated state code via parameterized query:**
```python
authorized_state = _get_authorized_state_for_user(current_user)
query = "SELECT * FROM state_person WHERE state_code = @state_code"
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter(
            "state_code", "STRING", authorized_state.value
        )
    ]
)
```

---

## SQL Injection

SQL injection is only exploitable when a query incorporates input from **end
users via HTTP requests**. The vast majority of SQL in this codebase is in ETL
pipelines, BigQuery view builders, ingest view definitions, calculators,
Airflow DAGs, and notebooks — all of which construct queries from
developer-controlled values (state codes, table names, column names, enum
values, config). That SQL is **not** exploitable and should not be treated as a
SQL injection risk.

Flag SQL injection only when **all three** of these conditions are true:

1. A value in the query originates from an HTTP request (form field, URL
   parameter, JSON body, header).
2. The surrounding code is user-facing — a Flask route, an API handler, an
   admin panel endpoint.
3. There is a realistic path for an external user to inject SQL.

**Do not flag SQL string construction in:**
- `recidiviz/calculator/` — ETL query builders
- `recidiviz/ingest/` — ingest view definitions
- `recidiviz/pipelines/` — Beam pipeline SQL
- `recidiviz/aggregated_metrics/` — metric view builders
- `recidiviz/validation/` — validation query builders
- `recidiviz/big_query/` — BigQuery utilities
- `recidiviz/task_eligibility/` — task eligibility criteria SQL
- `recidiviz/workflows/` — workflow ETL queries
- `recidiviz/airflow/` — DAG definitions
- Jupyter notebooks (`*.ipynb`)
- Any code that assembles SQL from hardcoded strings, enums, or config

**Bad — user input in query inside a Flask route:**
```python
@app.route('/api/search')
def search():
    q = request.args.get('query')
    query = f"SELECT * FROM table WHERE name = '{q}'"
```

**Good — parameterized:**
```python
@app.route('/api/search')
def search():
    q = request.args.get('query')
    query = "SELECT * FROM table WHERE name = @name"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", q)
        ]
    )
```

---

## Authentication and Authorization

Our auth surfaces:

- **Admin panel** — OAuth2 via Google Workspace
- **API endpoints** — Service account or Auth0-issued JWTs
- **Airflow DAGs** — Cloud Composer IAM
- **BigQuery** — row-level security via authorized views

Rules:

- Every user-facing endpoint must require authentication. Internal endpoints
  are not a substitute for auth — internal networks get misconfigured.
- Authorization must be enforced server-side. Client-side checks are cosmetic.
- Never trust an `Authorization` header without verifying the signature and
  issuer.
- Use `@requires_auth` and `@requires_permission(...)` decorators on Flask
  routes; do not inline auth checks.

**Bad — no auth check on a sensitive endpoint:**
```python
@app.route('/api/sensitive-data')
def get_sensitive_data():
    return jsonify(query_database())
```

**Good — decorated with auth and permission checks:**
```python
@app.route('/api/sensitive-data')
@requires_auth
@requires_permission('read:sensitive_data')
def get_sensitive_data():
    return jsonify(query_database())
```

---

## Secrets Management

- Production secrets live in **Google Secret Manager**. Access via
  `recidiviz.utils.secrets.get_secret(...)`.
- Development secrets live in `.env` files (gitignored). `.env.example` may
  contain placeholder keys but never real values.
- Airflow secrets live in Composer environment variables or Secret Manager.
- Never log secrets, include them in error messages, or emit them in telemetry.
- Never put a secret in a comment, including as an "example" — examples have
  been committed as real credentials many times across the industry.

**Bad — hardcoded credential:**
```python
API_KEY = "sk_live_abcd1234"
```

**Bad — committed config file:**
```python
config = {"database_password": "MyP@ssw0rd123"}
```

**Good — Secret Manager lookup:**
```python
from recidiviz.utils.secrets import get_secret
api_key = get_secret("external_api_key")
```

**Good — environment variable for local/dev:**
```python
api_key = os.environ["API_KEY"]
```

---

## Webhook and External Callback Signatures

We receive webhook and callback traffic from state partners and third-party
services. All such endpoints must verify signatures before processing the
payload.

- Validate signatures using the vendor-specified HMAC and shared secret
  (pulled from Secret Manager).
- Use constant-time comparison (`hmac.compare_digest`) — never `==`.
- Reject requests with missing signatures, wrong signatures, or replayed
  timestamps (where the vendor supports it).

**Bad — endpoint trusts the payload:**
```python
@app.route('/webhooks/partner', methods=['POST'])
def partner_webhook():
    process_event(request.json)
    return '', 200
```

**Good — signature verified before processing:**
```python
@app.route('/webhooks/partner', methods=['POST'])
def partner_webhook():
    signature = request.headers.get('X-Partner-Signature', '')
    expected = hmac.new(
        get_secret('partner_webhook_secret').encode(),
        request.get_data(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected):
        abort(401)
    process_event(request.json)
    return '', 200
```

---

## File Uploads and Path Handling

The admin panel and several ingest tools accept file uploads. Treat all
user-supplied paths and filenames as hostile.

- Never pass an uploaded filename directly into a filesystem path or GCS
  object name without sanitization.
- Never resolve a path derived from user input against a trusted base directory
  without confirming the result stays within that directory.
- Validate uploaded file types against an allowlist (content type and magic
  bytes, not just extension).

**Bad — path traversal via uploaded filename:**
```python
filename = request.files['upload'].filename
path = os.path.join('/var/uploads', filename)  # filename could be '../../etc/passwd'
request.files['upload'].save(path)
```

**Good — sanitized filename, confined path:**
```python
from werkzeug.utils import secure_filename
filename = secure_filename(request.files['upload'].filename)
path = os.path.join('/var/uploads', filename)
request.files['upload'].save(path)
```

---

## Cloud Storage Access

We write extensively to GCS. Access controls and bucket policies must be
enforced at the GCS layer, not at the application layer.

- Never make a GCS bucket public unless it is an intentional public asset
  bucket (and even then, review the policy).
- Never generate signed URLs with unnecessarily long expirations; scope them
  to the minimum duration required.
- Never grant `allUsers` or `allAuthenticatedUsers` IAM on buckets that hold
  state data or PII.
- Use uniform bucket-level access where possible; avoid per-object ACLs.

---

## Template Rendering and Output Encoding

We use Jinja2 in a few places (admin panel, email generation, some report
rendering). Jinja's autoescape protects against XSS **only** when enabled.

- Confirm autoescape is on for any `Environment(...)` that renders HTML.
- Never use `|safe` on a value that originated from user input.
- Never build SQL via Jinja templating with user-supplied parameters — use
  BigQuery parameterized queries or the `QueryBuilder` utilities.
- Never build shell commands via `str.format` or f-strings with user input —
  use `subprocess.run(args_list, shell=False)`.

---

## CORS and Cross-Origin Requests

- Never use `origin: "*"` (or equivalent) in deployed environments.
- Allowlist specific origins for production and staging.
- Local development may use permissive CORS for convenience; this must be
  gated on `environment == 'development'`.

---

## CSRF Protection

The admin panel and any form-based user-facing endpoints must include CSRF
protection for state-changing requests. API endpoints authenticated via
`Authorization: Bearer` tokens (not cookies) are generally not CSRF-vulnerable
because browsers will not auto-attach the header to cross-origin requests, but
confirm this on a case-by-case basis.

- Flask routes that accept `POST`/`PUT`/`PATCH`/`DELETE` and rely on session
  cookies must validate a CSRF token.
- Do not disable CSRF protection globally.

---

## Dependency Security

- Pin dependency versions in production code.
- Do not install packages with known high or critical CVEs — find an
  alternative or escalate to Peter or Dan.
- Pay extra attention to dependencies that handle authentication, PII,
  cryptography, or have native extensions.
- New packages with very low download counts (hundreds/week) should be
  discussed before adding — supply chain risk is real.

---

## Compliance Context

Our state partners' audits scrutinize: encryption in transit and at rest,
access control and auditing, incident response capability, third-party risk
management. When writing or reviewing code, flag issues that would fail a
state partner audit.

Some states require CJIS compliance, some require NIST framework alignment,
most require regular penetration testing. If a change materially affects the
security posture (new external integration, new data egress path, new auth
surface), flag it and loop in Peter or Dan.
