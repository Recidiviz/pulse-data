#!/usr/bin/env bash
#
# set_bq_project_default_cmek.sh
#
# Sets (or rolls back) the project-level default CMEK key for BigQuery.
# Once set, ALL BQ operations (queries, DDL, DML, loads) automatically use
# the CMEK key without needing --destination_kms_key on every job.
#
# This is critical for Assured Workloads compliance: the restrictNonCmekServices
# org policy blocks all BQ query jobs that don't specify a CMEK key. The
# project-level default satisfies this policy automatically.
#
# HOW IT WORKS
# ============
#
# Uses ALTER PROJECT SET OPTIONS to set (or clear) the
# `region-{REGION}.default_kms_key_name` option on the target project.
# This covers:
#   - Query result temp tables (anonymous datasets)
#   - New tables created via DDL (CREATE TABLE, CREATE TABLE AS SELECT)
#   - DML operations (INSERT, UPDATE, DELETE)
#   - Load jobs
#   - Copy jobs
#
# The option is PER REGION, and a KMS key can only encrypt data in its own
# location — so each region the project uses gets its own key and its own
# ALTER PROJECT statement. recidiviz-staging holds datasets in five locations
# (1,378 in `us`, plus ~10 across us-east1 / us-central1 / us-west1 /
# us-east4), all covered by the BQ_REGIONS list below.
#
# Existing tables are NOT affected — they retain their current encryption.
# To migrate existing non-CMEK tables, use `bq cp --destination_kms_key`.
#
# PREREQUISITES
# =============
#
# - gcloud CLI authenticated with BigQuery Admin on the target project
# - Access to cmek-82ade411-5705-4461-b2cb-9 (the Data-CJIS CMEK key project)
# - The BQ encryption service agent must have cryptoKeyEncrypterDecrypter on the key
#   (this script grants it automatically)
#
# USAGE
# =====
#
#   # Dry run (default) — shows what would happen:
#   ./set_bq_project_default_cmek.sh <project-id> [--apply]
#
#   # Apply the project default CMEK:
#   ./set_bq_project_default_cmek.sh recidiviz-staging --apply
#
#   # Rollback — remove the project default CMEK:
#   ./set_bq_project_default_cmek.sh recidiviz-staging --rollback
#
#   # Dry run the rollback:
#   ./set_bq_project_default_cmek.sh recidiviz-staging --rollback --dry-run
#
# Part of Recidiviz/zenhub-tasks#2606
# Part of the recidiviz-staging CJIS migration (#2335)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <project-id> [--apply | --rollback [--dry-run]]"
    echo ""
    echo "  project-id  GCP project (e.g. recidiviz-staging, recidiviz-123)"
    echo "  --apply     Set the project default CMEK (default is dry-run)"
    echo "  --rollback  Remove the project default CMEK"
    echo "  --dry-run   With --rollback, show what would happen without changing"
    exit 1
fi

PROJECT_ID="$1"
shift

CMEK_PROJECT="cmek-82ade411-5705-4461-b2cb-9"
# Every BigQuery location the target project holds datasets in. The
# default_kms_key_name option is per region, so each entry gets its own key
# (same name, different location) and its own ALTER PROJECT statement.
BQ_REGIONS=("us" "us-east1" "us-central1" "us-west1" "us-east4")
KEYRING_NAME="data-cjis"
KEY_NAME="${PROJECT_ID}-bq-default"

APPLY=false
ROLLBACK=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply)    APPLY=true; shift ;;
        --rollback) ROLLBACK=true; shift ;;
        --dry-run)  DRY_RUN=true; shift ;;
        *)          echo "Unknown flag: $1"; exit 1 ;;
    esac
done

# Determine mode:
#   (no flags)          → dry-run apply
#   --apply             → execute apply
#   --rollback          → execute rollback
#   --rollback --dry-run → dry-run rollback
if $ROLLBACK && $DRY_RUN; then
    MODE="rollback-dry-run"
elif $ROLLBACK; then
    MODE="rollback"
elif $APPLY; then
    MODE="apply"
else
    MODE="dry-run"
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { echo "[$(date +%H:%M:%S)] $*"; }

key_path() {
    local region="$1"
    echo "projects/${CMEK_PROJECT}/locations/${region}/keyRings/${KEYRING_NAME}/cryptoKeys/${KEY_NAME}"
}

run_bq_ddl() {
    local region="$1"
    local ddl="$2"
    # For ALTER PROJECT DDL, we may need to use an existing CMEK key for the
    # query job itself if the org policy is already enforced. Try without first,
    # fall back to with.
    if bq query --project_id="$PROJECT_ID" --location="$region" \
        --use_legacy_sql=false "$ddl" 2>/dev/null; then
        return 0
    fi
    log "  Retrying with explicit destination_kms_key..."
    bq query --project_id="$PROJECT_ID" --location="$region" \
        --use_legacy_sql=false \
        --destination_kms_key="$(key_path "$region")" "$ddl" 2>&1
}

# Prints the current project-level default key for one region, or nothing if
# unset. Reads PROJECT_OPTIONS, not EFFECTIVE_PROJECT_OPTIONS — the EFFECTIVE_
# view lags behind ALTER PROJECT by minutes and misreports fresh changes.
# The query itself may fail if the org policy is enforced and no default is
# set — that's fine, it means there's no default.
current_key_for_region() {
    local region="$1"
    local out
    out=$(bq query --project_id="$PROJECT_ID" --location="$region" \
        --use_legacy_sql=false --format=csv --quiet \
        "SELECT option_value FROM \`region-${region}\`.INFORMATION_SCHEMA.PROJECT_OPTIONS WHERE option_name = 'default_kms_key_name'" 2>&1 || true)
    echo "$out" | grep "projects/.*/cryptoKeys/" | head -1 | tr -d '[:space:]' || true
}

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

log "Project:     ${PROJECT_ID}"
log "CMEK project: ${CMEK_PROJECT}"
log "Key name:    ${KEY_NAME} (one per region: ${BQ_REGIONS[*]})"
log "Mode:        ${MODE}"
echo ""

log "Checking current project-level BQ default encryption (per region)..."
PREVIOUS_KEYS=()
for i in "${!BQ_REGIONS[@]}"; do
    region="${BQ_REGIONS[$i]}"
    cur="$(current_key_for_region "$region")"
    PREVIOUS_KEYS[i]="$cur"
    log "  ${region}: ${cur:-(none — Google-managed encryption)}"
done
echo ""

# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------

if $ROLLBACK; then
    ROLLED_BACK_ANY=false
    for i in "${!BQ_REGIONS[@]}"; do
        region="${BQ_REGIONS[$i]}"
        if [[ -z "${PREVIOUS_KEYS[$i]}" ]]; then
            log "[${region}] No project default CMEK is set — nothing to roll back."
            continue
        fi

        DDL="ALTER PROJECT \`${PROJECT_ID}\` SET OPTIONS (\`region-${region}.default_kms_key_name\` = NULL)"

        if [[ "$MODE" == "rollback-dry-run" ]]; then
            log "[${region}] [DRY RUN] Would run:"
            log "  ${DDL}"
            continue
        fi

        log "[${region}] Rolling back — removing project default CMEK..."
        run_bq_ddl "$region" "$DDL"
        log "[${region}] Done."
        ROLLED_BACK_ANY=true
    done

    if [[ "$MODE" == "rollback-dry-run" ]]; then
        log ""
        log "This removes the project-level defaults. BQ operations will revert"
        log "to requiring explicit --destination_kms_key (or will use"
        log "Google-managed encryption if the org policy allows it)."
        log ""
        log "To execute: $0 $PROJECT_ID --rollback"
    elif $ROLLED_BACK_ANY; then
        log ""
        log "Done. Project default CMEK has been removed."
        log "Note: tables created while the defaults were set keep their CMEK."
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

if [[ "$MODE" == "dry-run" ]]; then
    log "=== DRY RUN MODE — no changes will be made ==="
    echo ""
fi

PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
BQ_SA="bq-${PROJECT_NUMBER}@bigquery-encryption.iam.gserviceaccount.com"
ROTATION_PERIOD="7776000s"  # 90 days

if [[ "$MODE" != "dry-run" ]]; then
    # Ensure the BQ encryption service agent exists (once, not per region)
    gcloud beta services identity create --service=bigquery.googleapis.com \
        --project="$PROJECT_ID" > /dev/null 2>&1 || true
fi

for region in "${BQ_REGIONS[@]}"; do
    KEY_PATH="$(key_path "$region")"

    # Step 1: Ensure the KMS keyring exists in this region
    log "[${region}] Step 1/4: Ensuring KMS keyring exists..."
    if [[ "$MODE" == "dry-run" ]]; then
        log "  [DRY RUN] Would create keyring ${KEYRING_NAME} in ${region} (no-op if exists)"
    else
        if ! gcloud kms keyrings create "$KEYRING_NAME" \
            --location="$region" \
            --project="$CMEK_PROJECT" 2>/dev/null; then
            true  # Already exists — expected
        fi
        log "  Done"
    fi

    # Step 2: Create the crypto key in this region
    log "[${region}] Step 2/4: Ensuring crypto key exists..."
    if [[ "$MODE" == "dry-run" ]]; then
        log "  [DRY RUN] Would create key ${KEY_NAME} (no-op if exists)"
    else
        NEXT_ROTATION=$(date -u -v+90d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
            || date -u -d "+90 days" +"%Y-%m-%dT%H:%M:%SZ")
        if ! gcloud kms keys create "$KEY_NAME" \
            --keyring="$KEYRING_NAME" \
            --location="$region" \
            --project="$CMEK_PROJECT" \
            --purpose=encryption \
            --rotation-period="$ROTATION_PERIOD" \
            --next-rotation-time="$NEXT_ROTATION" \
            --protection-level=software 2>/dev/null; then
            true  # Already exists — expected
        fi
        log "  Done"
    fi

    # Step 3: Grant BQ encryption service agent access to this region's key
    log "[${region}] Step 3/4: Granting BQ encryption service agent access..."
    if [[ "$MODE" == "dry-run" ]]; then
        log "  [DRY RUN] Would grant cryptoKeyEncrypterDecrypter to ${BQ_SA}"
    else
        # This grant must not fail silently: with `set -e`, an unguarded failure
        # here would abort the script with no output at all (stderr is discarded),
        # leaving the operator with no clue why it stopped. Capture the output and
        # surface the error instead. The grant is idempotent, so any failure is a
        # real failure -- there is no benign "already exists" case to swallow.
        if ! GRANT_OUTPUT=$(gcloud kms keys add-iam-policy-binding "$KEY_NAME" \
            --keyring="$KEYRING_NAME" \
            --location="$region" \
            --project="$CMEK_PROJECT" \
            --role="roles/cloudkms.cryptoKeyEncrypterDecrypter" \
            --member="serviceAccount:${BQ_SA}" \
            --quiet 2>&1); then
            log "ERROR: failed to grant cryptoKeyEncrypterDecrypter to ${BQ_SA}:"
            log "  ${GRANT_OUTPUT}"
            log "Aborting before setting this region's project default."
            log "Regions before ${region} in (${BQ_REGIONS[*]}) may already be set."
            exit 1
        fi
        log "  Done"
    fi

    # Step 4: Set the project-level default for this region
    DDL="ALTER PROJECT \`${PROJECT_ID}\` SET OPTIONS (\`region-${region}.default_kms_key_name\` = '${KEY_PATH}')"

    log "[${region}] Step 4/4: Setting project-level default CMEK..."
    if [[ "$MODE" == "dry-run" ]]; then
        log "  [DRY RUN] Would run:"
        log "  ${DDL}"
    else
        run_bq_ddl "$region" "$DDL"
        log "  Done"
    fi
done

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

echo ""
if [[ "$MODE" != "dry-run" ]]; then
    log "Verifying project defaults (per region; enforcement can lag ~30-120s)..."
    for region in "${BQ_REGIONS[@]}"; do
        VERIFIED=false
        for _attempt in 1 2 3; do
            if current_key_for_region "$region" | grep -q "$KEY_NAME"; then
                VERIFIED=true
                break
            fi
            sleep 10
        done
        if $VERIFIED; then
            log "  ${region}: VERIFIED"
        else
            log "  ${region}: not visible yet — verify manually:"
            log "    bq query --project_id=${PROJECT_ID} --location=${region} --use_legacy_sql=false \\"
            log "      'SELECT * FROM \`region-${region}\`.INFORMATION_SCHEMA.PROJECT_OPTIONS'"
        fi
    done
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
log "========================================="
log "SUMMARY"
log "========================================="
log "Project:     ${PROJECT_ID}"
log "Action:      ${MODE}"
for i in "${!BQ_REGIONS[@]}"; do
    region="${BQ_REGIONS[$i]}"
    log "  ${region}:"
    log "    previous: ${PREVIOUS_KEYS[$i]:-(none)}"
    log "    now:      $(key_path "$region")"
done
log "========================================="

if [[ "$MODE" == "dry-run" ]]; then
    echo ""
    log "This was a DRY RUN. To apply:"
    log "  $0 $PROJECT_ID --apply"
    echo ""
    log "To rollback later:"
    log "  $0 $PROJECT_ID --rollback"
fi
