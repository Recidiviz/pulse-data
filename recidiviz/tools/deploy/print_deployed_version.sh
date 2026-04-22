#!/usr/bin/env bash
# Prints the currently-deployed version of pulse-data for the given GCP
# project. Each signal is resolved independently from the project id — no
# signal is derived from another signal's output. View-update version is
# always last because it lags every other signal (it only advances after
# update_managed_views_all re-materializes the view graph, ~2–3 hours
# post-deploy).
#
#   1. docker_image_tag in Terraform state — updated mid-deploy, as soon as
#      `terraform apply` writes a new version. This is what the Docker image,
#      Cloud Run services, Cloud Run jobs, and Composer env reference. Read
#      directly from `gs://<project>-tf-state/default.tfstate` (no `terraform
#      init`), so it returns in ~1–2s. Resolved for any project id.
#
#   2. Latest version tag on the branch that feeds this project:
#        - staging (recidiviz-staging): latest tag on `origin/main` (every
#          staging deploy tags the tip of main).
#        - prod (recidiviz-123): skipped. Release branches often get tagged
#          well in advance of the prod deploy, so branch tip doesn't tell
#          you what's running in prod. A better prod-side second source of
#          truth is future work.
#
#   3. Cloud Run revision history on case-triage-web (us-central1), which
#      serves as our per-deploy history because every release bumps
#      docker_image_tag and produces a new revision. Each revision's
#      metadata.name has the deploy commit's git short SHA as its suffix
#      (see cloud-run.tf — the name is
#      `case-triage-web-${local.git_short_hash}`), so we can resolve each
#      revision to a tag via `git tag --points-at <sha>`. This is how we
#      recover BOTH the current deployed tag AND the *previous* deployed
#      tag — walking backwards through revisions until the tag changes.
#      Resolved for any project id. Note: revisions that predate the
#      `<service>-<hash>` naming convention (auto-generated names like
#      `case-triage-web-00765-skz`) surface as "unknown" entries.
#
#   4. data_platform_version from the most recent successful row in
#      view_update_metadata.per_view_update_stats (populated by the
#      update_managed_views_all Airflow task). Lags the code deploy by ~2–3
#      hours while the full view graph is re-materialized; relevant for
#      data investigations since BQ view contents still reflect the prior
#      version until this catches up. If it doesn't catch up, the view
#      update task is probably failing. Resolved for any project id.
#
# Usage:
#   print_deployed_version.sh <project_id>
#     e.g. print_deployed_version.sh recidiviz-123
#          print_deployed_version.sh recidiviz-staging

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <project_id>" >&2
    exit 1
fi

PROJECT_ID=$1
BASH_SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${BASH_SOURCE_DIR}/../../.." && pwd)

# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy_helpers.sh"

# Fetch tags once up front — both the branch-tip (staging) and Cloud Run
# signals use them.
echo "Fetching tags from origin..." >&2
git -C "${REPO_ROOT}" fetch --quiet --tags origin

# 1. TF state — resolved for any project id.
TF_VERSION="$(last_deployed_version_tag "${PROJECT_ID}")"

# 2. Branch-tip tag — driven off project id only.
BRANCH_LABEL=""
BRANCH_TAG=""
case "${PROJECT_ID}" in
    recidiviz-staging)
        BRANCH_LABEL="origin/main"
        BRANCH_TAG="$(last_version_tag_on_branch "origin/main" "true" "${REPO_ROOT}")"
        ;;
    recidiviz-123)
        # No reliable branch-tip signal for prod — see header comment.
        ;;
    *)
        # Unknown project — skip the branch-tip signal rather than guess.
        echo "Unknown project [${PROJECT_ID}]; skipping branch-tip signal." >&2
        ;;
esac

# 3. Cloud Run revisions — current + previous deployed tag.
#
# Resolve each revision's short SHA to a tag; walk forward until we find a
# tag different from the current one (that's `prev`). If a revision's SHA
# doesn't resolve to a tag (untagged deploy commit or older revision naming
# convention), fall back to annotating the SHA.
CR_CURRENT_TAG=""
CR_CURRENT_TS=""
CR_PREV_TAG=""
CR_PREV_TS=""
while IFS=$'\t' read -r suffix timestamp; do
    # Only try tag resolution if the suffix looks like an 8-char hex SHA.
    if [[ "${suffix}" =~ ^[0-9a-f]{8}$ ]]; then
        tag="$(git -C "${REPO_ROOT}" tag --points-at "${suffix}" 2>/dev/null | head -1)"
        if [[ -z "${tag}" ]]; then
            tag="(${suffix}: no tag)"
        fi
    else
        tag="(${suffix}: pre-hash revision naming)"
    fi

    if [[ -z "${CR_CURRENT_TAG}" ]]; then
        CR_CURRENT_TAG="${tag}"
        CR_CURRENT_TS="${timestamp}"
    elif [[ "${tag}" != "${CR_CURRENT_TAG}" ]]; then
        CR_PREV_TAG="${tag}"
        CR_PREV_TS="${timestamp}"
        break
    fi
done < <(recent_deploy_cloud_run_revisions "${PROJECT_ID}" 10)

# 4. Latest view-update version — resolved for any project id, via the
# same code path that _check_latest_view_update() uses inside
# load_views_to_sandbox.
VIEW_UPDATE_OUTPUT=$(uv run python -m recidiviz.tools.deploy.print_latest_view_update_version --project_id "${PROJECT_ID}" 2>/dev/null)
VIEW_UPDATE_VERSION=$(echo "${VIEW_UPDATE_OUTPUT}" | cut -f1)
VIEW_UPDATE_TIMESTAMP=$(echo "${VIEW_UPDATE_OUTPUT}" | cut -f2)

echo
echo "Project: ${PROJECT_ID}"
echo
printf "  Deployed code version (TF state):   %s\n" "${TF_VERSION}"
if [[ -n "${BRANCH_TAG}" ]]; then
    printf "  Latest tag on %-21s %s\n" "${BRANCH_LABEL}:" "${BRANCH_TAG}"
else
    printf "  Latest tag on branch:               (n/a — no reliable branch-tip signal for this project)\n"
fi
printf "  Cloud Run current (%s):   %s (deployed %s)\n" "${DEPLOY_HISTORY_CLOUD_RUN_SERVICE}" "${CR_CURRENT_TAG}" "${CR_CURRENT_TS}"
if [[ -n "${CR_PREV_TAG}" ]]; then
    printf "  Cloud Run previous (%s):  %s (deployed %s)\n" "${DEPLOY_HISTORY_CLOUD_RUN_SERVICE}" "${CR_PREV_TAG}" "${CR_PREV_TS}"
else
    printf "  Cloud Run previous (%s):  (no previous version found in recent revision history)\n" "${DEPLOY_HISTORY_CLOUD_RUN_SERVICE}"
fi
printf "  Latest view-update version:         %s (completed %s)\n" "${VIEW_UPDATE_VERSION}" "${VIEW_UPDATE_TIMESTAMP}"
echo

if [[ -n "${BRANCH_TAG}" && "${TF_VERSION}" != "${BRANCH_TAG}" ]]; then
    echo "NOTE: TF-state version and branch-tip tag differ — either a deploy"
    echo "is in progress (a newer tag was cut but TF hasn't applied it yet),"
    echo "or TF applied ahead of a tag being cut. Both windows are normally"
    echo "short."
fi

if [[ "${TF_VERSION}" != "${CR_CURRENT_TAG}" ]]; then
    echo "NOTE: TF-state version and Cloud Run current tag differ — a deploy"
    echo "is probably in progress (either TF has applied but Cloud Run hasn't"
    echo "finished rolling a new revision, or vice versa)."
fi

if [[ "${TF_VERSION}" != "${VIEW_UPDATE_VERSION}" ]]; then
    echo "NOTE: TF-state version and view-update version differ. Expected"
    echo "for up to ~3 hours post-deploy while views are being re-"
    echo "materialized. If this persists, update_managed_views_all may be"
    echo "failing — check Airflow / PD."
fi
