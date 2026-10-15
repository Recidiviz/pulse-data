# Threat model: Recidiviz/pulse-data

The scanner reads this file before it starts. It covers the public mirror of Recidiviz's
data platform. The security policy is the org-wide SECURITY.md on GitHub; reports go to
security@recidiviz.org.

## What this project does and where untrusted input enters

Recidiviz builds tools for state corrections agencies. pulse-data ingests records from each
state, computes metrics in BigQuery, and serves Flask APIs to state staff. Every record is
about a real person under supervision or in custody. Confidentiality is the primary goal, and
the boundary that matters most is the one between states: Tennessee staff must never see
Idaho data, and one state's upload must never alter another state's records.

### Where the code is

- `/src` is the checkout, with the virtual environment at `/src/.venv` on `PATH`.
- This repository is a mirror of a private repository. The mirror omits
  `recidiviz/calculator`, the raw data configs, the ingest views, and a few other directories.
  Many modules import `recidiviz.calculator`, so the two Flask servers and some route modules
  raise ModuleNotFoundError here. That is a mirror limitation, not a finding. The route,
  authorization, and persistence code is still present and reviewable.

### Entry points

Unauthenticated:

- Twilio webhooks: `/webhook/twilio_status` and `/webhook/twilio_incoming_message` in
  `recidiviz/case_triage/workflows/workflows_routes.py` and
  `recidiviz/case_triage/jii/jii_texts_routes.py`. The signature check is
  `recidiviz/case_triage/workflows/twilio_validation.py` and
  `recidiviz/case_triage/helpers.py`.
- Edovo course completions: `POST /course-completions` in
  `recidiviz/case_triage/edovo/edovo_routes.py`. A vendor posts with a Google access token
  minted through Workload Identity Federation; `recidiviz/case_triage/edovo/wif_verifier.py`
  checks it against Google's tokeninfo endpoint. The body carries person identifiers and
  names and is untrusted.
- The Iowa early discharge status webhook `/webhook/mcp_us_ia_early_discharge_status` in
  `workflows_routes.py`.
- Cloud Tasks callbacks: the `handle_*` routes in `workflows_routes.py` are meant to be
  reached only by Cloud Tasks (`g.is_cloud_task_request`). A caller who reaches one directly
  with a crafted payload crosses a boundary.
- GCS object notifications to `recidiviz/application_data_import/server.py`, which carry a
  state code and an object name.
- `/health` on every server.

Authenticated with an Auth0 bearer JWT (state staff):

- Case Triage backend, `recidiviz/case_triage/server.py`. Blueprints: Workflows
  (`workflows_routes.py`), Insights (`outliers/outliers_routes.py`), Pathways
  (`shared_pathways/shared_pathways_blueprint.py`), sentence calculation, and Public
  Pathways (`recidiviz/public_pathways/public_pathways_routes.py`). Every blueprint runs
  `build_authorization_handler` from `recidiviz/case_triage/authorization_utils.py` in a
  `before_request` hook. The route's `<state>` segment, the pseudonymized ids in the path,
  query strings, and JSON bodies are all caller-controlled.
- Justice Counts Publisher backend, `recidiviz/justice_counts/control_panel/server.py`.
  Agency users log in with Auth0. The admin routes in
  `recidiviz/justice_counts/control_panel/routes/admin.py` manage users, agencies, vendors,
  and metric settings. Agency users upload spreadsheets that
  `recidiviz/justice_counts/bulk_upload/` parses; the file contents are untrusted.

Authenticated with a Google Identity-Aware Proxy JWT (Recidiviz staff only):

- Admin Panel backend, `recidiviz/admin_panel/server.py`. `recidiviz/utils/auth/gce.py`
  validates the `X-Goog-IAP-JWT-Assertion` header on every request except `/health`. It
  serves `/admin`, `/auth` (`recidiviz/auth/auth_endpoint.py`), and `/auth/users`
  (`recidiviz/auth/auth_users_endpoint.py`), which set every state user's permissions.

Semi-trusted partner data:

- Raw CSV files that states upload to GCS, read by `recidiviz/ingest/direct`. A contracted
  partner produces them, but their systems do, so a malformed file must fail closed.

### Trust model

Actors, from least to most privileged:

1. **Anonymous**: untrusted.
2. **A state user**: an Auth0 token whose `app_metadata.stateCode` names one state, for
   example `US_TN`. They may read only that state, and only the routes and records their
   `routes`, `featureVariants`, and pseudonymized id allow. In Insights, a supervisor sees
   their own officers' outcomes, not every supervisor's.
3. **A CSG user**: `stateCode` `CSG`, limited to `csg_enabled_states`.
4. **A Recidiviz user**: `stateCode` `RECIDIVIZ`, limited to the states in
   `allowedStates`. Not every Recidiviz user may see every state.
5. **A Justice Counts agency user**: may read and change only agencies they belong to.
6. **Vendors**: Twilio by signature, Edovo by WIF token, Cloud Tasks by request origin. Each
   may call only its own routes.
7. **A Recidiviz admin** behind IAP: trusted to configure users, roles, and product
   configurations for every state. An admin-only issue matters only when it reaches the
   host, reads a secret the API is designed to hide, or runs code.
8. **Operator input**: Secret Manager, the environment, Terraform, and the GCP metadata server
   are trusted.

The boundaries a finding must cross, in order of importance:

- **State to state.** Any path by which data for one state reaches a user of another.
- **User to user inside a state.** Reading a record, outcome, or event a user is not entitled
  to by their role or caseload, including through a cache or an export.
- **Vendor route to user route.** A webhook or Cloud Tasks payload that acts as a user, or a
  user request that enqueues a task a vendor should have sent.
- **Agency to agency** in Justice Counts.
- **Staging to production.** The Admin Panel pushes configurations from staging to
  `admin-panel-prod.recidiviz.org` in `recidiviz/admin_panel/routes/outliers.py` and
  `routes/workflows.py`. The authorization header comes from `auth_header_for_request_to_prod`.
  A path that lets a non-admin trigger that push crosses this boundary.

## Components that matter most and least

Highest priority, with the files to read first:

1. Authorization. `recidiviz/case_triage/authorization_utils.py`
   (`on_successful_authorization_requested_state`), `recidiviz/utils/auth/auth0.py`,
   `recidiviz/utils/auth/gce.py`, `recidiviz/utils/validate_jwt.py`, and every
   `*_authorization.py` next to a route module. Look for a route that reads from a state
   other than `<state>`, a pseudonymized id that is not checked against the caller, or a
   `RECIDIVIZ` user that bypasses `allowedStates`.
2. Route modules. `recidiviz/case_triage/workflows/workflows_routes.py` (the
   `external_request` routes write contact notes, send SMS and email, and submit forms to
   state systems), `recidiviz/case_triage/outliers/outliers_routes.py`,
   `recidiviz/case_triage/shared_pathways/`, `recidiviz/public_pathways/`,
   `recidiviz/auth/`, `recidiviz/admin_panel/routes/`, and
   `recidiviz/justice_counts/control_panel/routes/`.
3. Query construction. Any SQL or BigQuery text built from a request value: state codes,
   metric names, filters, pseudonymized ids. `recidiviz/persistence/database/`,
   `recidiviz/case_triage/shared_pathways/`, `recidiviz/outliers/querier/`, and
   `recidiviz/big_query/`.
4. Vendor entry points: the Twilio validation, `wif_verifier.py`, the Cloud Tasks checks, and
   the GCS notification handler.
5. Parsers of partner and user files: `recidiviz/justice_counts/bulk_upload/` and
   `recidiviz/ingest/direct/raw_data/`.
6. Outbound requests the server makes on a user's behalf: the Workflows `/proxy` route, the
   Google Routes call in `optimize_route`, `email_user`, and the staging-to-production push.
   See the by-design notes below before reporting `/proxy`.

Lower priority or out of scope:

- `recidiviz/tools/`: command-line scripts an engineer runs with their own credentials. Not
  deployed.
- `recidiviz/airflow/` and `recidiviz/pipelines/`: scheduled jobs. In scope only when an
  external input reaches them.
- `recidiviz/NOT_FOR_PRODUCTION_USE/`, `frontends/prototypes/`, and anything guarded by
  `in_development()` or `in_offline_mode()`.
- `frontends/admin-panel/`: in scope only for a finding that reaches the backend or another
  admin's session.
- Tests, fixtures, `name_lists/`, docs, and CI workflows.

## What counts as a finding

A report needs all four, as in the ERPNext and Windmill models:

- **Entry point**: the route or handler, with file and line.
- **Actor**: the least privileged actor that works. Use a state user before a Recidiviz user,
  and anonymous before either. Say which state the token names.
- **Input**: the request value and how it reaches the sink.
- **Impact**: what the actor reads, writes, sends, or runs that they should not. Name the
  boundary crossed.

One report per root cause, listing every route it reaches. Say whether sibling routes have
the same gap. Prove impact without maximizing it: read one record, do not dump a table.

## How to exercise it

- Tests mirror the source tree. `recidiviz/case_triage/workflows/workflows_routes.py` is
  tested by `recidiviz/tests/case_triage/workflows/workflows_routes_test.py`. Run one
  package with `pytest recidiviz/tests/<package> -p no:cacheprovider`. The default options in
  `setup.cfg` use pytest-xdist, which is installed.
- The image user is `recidiviz`. Tests that need a database start their own PostgreSQL
  with `pg_ctl` (`recidiviz/tools/postgres/local_postgres_helpers.py`); that fails as root.
- The BigQuery emulator image is not in the scanner image. Skip `recidiviz/tests/big_query`
  and anything that pulls the emulator.
- Route tests build a Flask test client and patch `build_authorization_handler` in the
  blueprint's module, so a test can run a route as any actor by supplying the claims it
  wants. `recidiviz/tests/case_triage/outliers/outliers_routes_test.py` shows the pattern.
  A pytest test in that style, with a state user's claims and the request that crosses a
  boundary, is the preferred reproducer.
- What collects in this mirror, measured on 2026-10-13 with `pytest --collect-only`
  (counts are tests; the error column is modules that fail to import):

  | Package under `recidiviz/tests/` | Collected | Import errors |
  | --- | --- | --- |
  | common | 459 | 0 |
  | utils | 288 | 0 |
  | persistence | 664 | 8 |
  | ingest/direct | 440 | 11 |
  | pipelines | 618 | 149 |
  | case_triage | 347 | 14 |
  | justice_counts | 144 | 0 |
  | public_pathways | 41 | 4 |
  | admin_panel | 25 | 8 |
  | cloud_functions | 25 | 0 |
  | workflows | 7 | 13 |
  | auth | 3 | 3 |
  | outliers | 0 | 5 |
  | task_eligibility | 0 | 21 |
  | application_data_import | 0 | 1 |

  Nearly every import error is `recidiviz.calculator` or a `views` package the mirror omits.
  Where a route's test module is one of them, review the route source and write the
  reproducer against the authorization helper or the query builder instead.

## How we rate severity

Severity follows the attacker's position and the boundary crossed, not the bug class.

- **Critical**: an anonymous caller, a vendor route, or any state user reads or changes
  another state's data; an authentication bypass on any server; remote code execution, or
  SSRF that reaches the GCP metadata server or returns a secret; deserialization of untrusted
  data with pickle, joblib, torch.load, or unsafe YAML; exposure of many person records.
- **High**: a state user reads or changes a record, outcome, or message inside their own
  state that their role or caseload does not cover; SQL or BigQuery injection with a valid
  user token; a missing or bypassable webhook signature or Cloud Tasks check; a user who can
  trigger the staging-to-production push; cross-site request forgery on a cookie-authenticated
  state-changing route; stored cross-site scripting that reaches another admin or another
  agency's user; a spreadsheet or CSV that alters another agency's or state's data.
- **Medium**: information disclosure without person data, such as stack traces, internal
  hostnames, or configuration; denial of service or rate-limit bypass from a single request;
  logging of a name, date of birth, identifier, or address, which is a policy violation here
  even without an attacker.
- **Low**: defects only reachable in `in_development()` or `in_offline_mode()` branches,
  in `recidiviz/tools`, or in a dependency with no reachable call path.

A Recidiviz admin behind IAP is highly trusted. An issue that needs that role is at most
medium unless it reaches the host or a hidden secret. A report that could not be reproduced
in this image, because the module is missing from the mirror, should say so; it is a lead
for the maintainers, not a confirmed finding, and is rated one level lower.

## Anything to leave alone

By design or already known. Do not report these on their own:

- The Workflows `/proxy` route sends a request with a caller-supplied method, headers, and
  body to a URL read from Secret Manager by name. The caller chooses the secret name, not the
  host. Report it only if a caller can pick the destination, read a secret value, or reach a
  URL secret that was not meant for that state.
- The `/ip` route calls curlmyip.org. It exists to show the egress address.
- The Workflows and Insights blueprints are exempt from Flask-WTF CSRF protection because
  they authenticate with a bearer JWT, not a cookie. That exemption is documented in
  `recidiviz/case_triage/server.py`.
- The Sentry DSN strings in the server modules are intentionally public.
- `client-secret-staging.json.enc` is SOPS-encrypted.
- `name_lists/` and the test fixtures hold synthetic people.
- The absence of `recidiviz/calculator`, the raw data configs, the ingest views, and the
  state-specific task eligibility criteria are deliberate mirror exclusions.
- Dependency versions are pinned in `uv.lock` and tracked separately. Report a dependency
  only with a reachable call path.
- Missing security headers, version disclosure, user enumeration on login, self-XSS, and
  volumetric denial of service.
- A 15-requests-per-second limiter already runs on the Case Triage backend. Its absence on a
  route is not a finding unless it enables one of the outcomes above.

## How reports and patches should look

Send reports to security@recidiviz.org, one per root cause. Give the commit, the entry point
with file and line, the actor and the state their token names, the input, the boundary
crossed, and a pytest reproducer in the style above where the module imports in this mirror.
Patches as a unified diff against `main`, fixing the check at the place that enforces it
rather than at one caller, with a regression test. The fix lands in the private repository
and mirrors back, so a small rebase may be needed. Use only synthetic names and identifiers
in a report; never copy a fixture row that looks real.
