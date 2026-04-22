# CLAUDE.md — Deploy tooling and versioning

Scripts in this directory handle releasing `pulse-data` to staging and
production. A single deploy ships **both application code (the Docker image)
and infrastructure (Terraform) together at the same version** — there is no
separate TF pipeline.

## Versioning scheme

| Thing | Pattern | Example |
|---|---|---|
| Release tag | `v<major>.<minor>.<patch>` | `v1.677.2` |
| Alpha tag (staging-only) | `v<major>.<minor>.0-alpha.<n>` | `v1.678.0-alpha.1` |
| Release branch | `releases/v<major>.<minor>-rc` | `releases/v1.677-rc` |
| Alpha branch | `alpha/v<major>.<minor>.0-alpha.<n>` | `alpha/v1.678.0-alpha.1` |

Development happens on `main`. Release tags live on `releases/v1.<N>-rc`
branches. Alpha tags are created from `main` at deploy time; each alpha
deploy also creates a same-named `alpha/...` branch as a reference point.

**Every release tag is deployed to staging first** (as a release candidate,
via `cut_release_candidate.sh`) and *then* promoted to prod (via
`deploy_production.sh`). So a release-shaped tag like `v1.678.0` is not
prod-exclusive — when the RC is cut at the end of a release week, that tag
first runs in staging for verification before being deployed to prod. In
practice this means **staging can be running either an alpha tag
(`v<N>.<M>.0-alpha.<X>`) or a release tag (`v<N>.<M>.<P>`) at any given
moment** — what matters is what's in the staging TF state. Only alpha tags
are staging-exclusive; release tags are shared between envs.

## What a single deploy includes

Every deploy script (including the "cut" scripts) does three things:

1. Builds and tags a Docker image at the commit being deployed.
2. Runs `terraform apply` against the target project, using the TF files
   **at that commit**, passing `docker_image_tag=<tag>`.
3. Runs post-deploy tasks.

Consequence: "what TF is live in a given env" equals "what TF is at the
deployed tag" — not what's on main. A TF-only change on main has no effect on
any env until a release containing it is deployed.

## Deploy scripts — what each one does

| Script | Invocation | Deploys to | Creates branch | New tag |
|---|---|---|---|---|
| `deploy_alpha_to_staging.sh` | off `main` | staging | `alpha/v<N>.0-alpha.<M>` | `v<N>.0-alpha.<M>` |
| `cut_release_candidate.sh main` | off `main` | staging | `releases/v<N>-rc` | `v<N>.0` |
| `cut_release_candidate.sh releases/v<N>-rc` | off release branch | staging | *(none — reuses existing branch)* | `v<N>.<patch+1>` |
| `deploy_production.sh <tag>` | off `releases/v<N>-rc` at the given tag | prod | *(none)* | *(uses existing tag)* |
| `deploy_local_to_staging.sh` | off local working tree | staging (dev only) | *(none)* | local-only tag |

Both `deploy_alpha_to_staging.sh` and `cut_release_candidate.sh` **deploy
to staging as part of running** — they're not just tag-and-branch tools.
`deploy_production.sh` is the only script that deploys to prod, and it only
deploys a tag that was previously created (usually by
`cut_release_candidate.sh`) and verified in staging.

## How to find the currently-deployed version

There is **no single source of truth**. Each signal below is resolved
independently — derived from the project id, not from any other signal's
output:

1. **Terraform state** — `docker_image_tag` in TF state, updated mid-deploy
   as soon as `terraform apply` writes a new version. This is what the
   Docker image, Cloud Run services/jobs, and Composer env reference.
   Resolved for any project.
   ```
   source recidiviz/tools/deploy/deploy_helpers.sh
   last_deployed_version_tag <project_id>   # e.g. recidiviz-123 or recidiviz-staging
   ```
   Under the hood: a direct `gsutil cat "gs://<project_id>-tf-state/default.tfstate" | jq '.outputs.docker_image_tag.value'`
   — no `terraform init`, so the read takes ~1–2s.

2. **Latest version tag on the branch that feeds this project**:
   - **Staging** (`recidiviz-staging`) → latest tag on `origin/main`. Every
     staging deploy (alpha or RC) tags the tip of main, so the newest tag
     on main is the latest candidate for staging.
   - **Prod** (`recidiviz-123`) → **no reliable branch-tip signal**.
     Release branches are often tagged well in advance of the prod
     deploy, so `origin/releases/vN.M-rc` tip doesn't tell you what's
     actually running in prod. A better prod-side second source of truth
     is future work.

3. **Cloud Run revisions on `case-triage-web`** — every prod release and
   every staging deploy bumps `docker_image_tag`, which changes the image
   URL on this service, which produces a new Cloud Run revision. The
   service's Terraform sets `revision.metadata.name` to
   `case-triage-web-${local.git_short_hash}` (see
   `recidiviz/tools/deploy/terraform/cloud-run.tf`), so the deploy commit's
   git short SHA is embedded in the revision name and resolves to a tag
   via `git tag --points-at <sha>`. Walking the revision list newest-first
   until the tag changes gives you the **previous deployed tag** — the
   only reliable programmatic source for that on prod today. Resolved for
   any project.

4. **Latest successful view update** — `data_platform_version` from the
   most recent row in `view_update_metadata.per_view_update_stats`. This
   lags the code deploy by ~2–3 hours while the `update_managed_views_all`
   Airflow task re-materializes the view graph. Important for data
   investigations: BigQuery view contents still reflect the prior version
   until this catches up. If it doesn't catch up, the view update task is
   probably failing. Resolved for any project. Always printed last because
   it's always the last signal to catch up.

The one-shot helper that prints the applicable signals:
```
./recidiviz/tools/deploy/print_deployed_version.sh <project_id>
```

- For **staging**, all four signals are printed. TF state == branch tag
  == Cloud Run current == view-update version → the env is fully settled
  on that version. TF state differing from Cloud Run current or branch
  tip → a deploy is in progress; the view-update lagging by < ~3 hours is
  expected post-deploy.
- For **prod**, the branch-tip row is `(n/a)` and the other three signals
  print as usual. The Cloud Run "previous" row is the most useful
  investigator tool — use it as `prev_deployed_tag` for
  `git log <prev>..<deployed>` scoping.

Airflow DAG runs are not yet tagged with the deployed version (future work).
For now, infer the code version from TF state or Cloud Run current and the
data-freshness version from the view-update table.

## Git idioms for "what changed in what's deployed"

When investigating a regression, scope "recent changes" queries to the
deployed tag, not `origin/main`:

```bash
# Is commit X live in the deployed version?  (exit 0 if yes)
git merge-base --is-ancestor <sha> <deployed_tag>

# What changed between the last two deployed versions?
git log --oneline <prev_tag>..<deployed_tag> -- <path>

# Read a file exactly as deployed:
git show <deployed_tag>:<path/to/file.py>
```

If an env-specific failure (e.g., prod fails, staging passes) has identical
deployed code in both envs, the divergence is almost certainly **infra
state**, not code. Compare live resources — `bq ls --connection`,
`gcloud sql instances list`, IAM, etc. — across projects before blaming a
commit.

## Cherry-picks (prod hotfixes)

A fix merged to `main` does **not** reach prod until the next release
series is cut. For hotfixes within the current release series, cherry-pick
onto the active `releases/v1.<N>-rc` branch using:

```
./recidiviz/tools/cherry-pick.sh <merged-pr-url>
```

That script creates a `{username}/cp-<PR#>` branch off the RC branch,
cherry-picks the merge commit from main, prompts for the required metadata
(issue description, cherry-pick type, etc.), and opens a `[CHERRYPICK]` PR
against the RC branch. Once merged, run
`cut_release_candidate.sh releases/v1.<N>-rc` to cut a new patch tag
(`v1.<N>.<patch+1>`) and deploy it to staging; after verification, run
`deploy_production.sh <tag>` to push to prod.

Separately, `main` typically also needs a forward fix so the bug doesn't
recur in the next release series — the fix on the RC branch does not
propagate back to main automatically.

## Common deploy operations

| Task | Script |
|---|---|
| Cut a new release branch off main | `cut_release_candidate.sh main` |
| Re-cut after cherry-picks | `cut_release_candidate.sh releases/v1.<N>-rc` |
| Deploy alpha from main to staging | `deploy_alpha_to_staging.sh` |
| Deploy a release tag to prod | `deploy_production.sh <tag>` |
| Create a cherry-pick PR | `recidiviz/tools/cherry-pick.sh <pr-url>` |
| Plan TF against a project | `terraform_plan.sh <project_id> [tf_state_prefix]` |
| Apply local code to staging (dev only) | `deploy_local_to_staging.sh` |
| Check CI status before deploy | `verify_github_check_statuses.py` |
| Print the deployed version of a project | `print_deployed_version.sh <project_id>` |

Shared helpers: `deploy_helpers.sh` (bash) and `terraform_helpers.py`
(Python). The Cloud Build-orchestrated stages live under `cloud_build/`.
