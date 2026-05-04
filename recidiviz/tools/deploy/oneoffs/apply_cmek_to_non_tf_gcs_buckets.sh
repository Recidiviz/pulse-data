#!/usr/bin/env bash
#
# apply_cmek_to_non_tf_gcs_buckets.sh
#
# Applies CMEK (Customer-Managed Encryption Keys) to all GCS buckets in a
# GCP project that are NOT already CMEK-encrypted. Terraform-managed buckets
# already have CMEK via the cloud-storage-bucket module (PRs #72428, #73344,
# #74169) and will be skipped automatically.
#
# This script handles the remaining non-TF buckets: developer scratch buckets,
# Cloud Functions auto-created buckets, Composer/Dataflow buckets, legacy
# buckets, etc.
#
# HOW IT WORKS
# ============
#
# For each bucket in the project:
#   1. Skip if it already has a CMEK key (idempotent — safe to re-run)
#   2. Look up the bucket's location (region or multi-region)
#   3. Create a gcs-cmek keyring in that location (no-ops if it already exists)
#   4. Create a per-bucket crypto key (no-ops if it already exists)
#   5. Grant the GCS service agent encrypt/decrypt on the key
#   6. Set the key as the bucket's default encryption key
#
# New objects written to the bucket will use CMEK. Existing objects remain
# encrypted with Google-managed keys (this is expected and acceptable for
# CJIS compliance — the org policy enforces CMEK on new writes).
#
# PREREQUISITES
# =============
#
# - gcloud CLI authenticated with access to the target project
# - Access to cmek-82ade411-5705-4461-b2cb-9 (the Data-CJIS CMEK key project)
#
# USAGE
# =====
#
#   # Dry run (default) — shows what would happen, changes nothing:
#   ./apply_cmek_to_non_tf_gcs_buckets.sh <project-id>
#
#   # Apply for real:
#   ./apply_cmek_to_non_tf_gcs_buckets.sh <project-id> --apply
#
#   # Examples:
#   ./apply_cmek_to_non_tf_gcs_buckets.sh recidiviz-staging
#   ./apply_cmek_to_non_tf_gcs_buckets.sh recidiviz-staging --apply
#   ./apply_cmek_to_non_tf_gcs_buckets.sh recidiviz-123 --apply
#
# Part of Recidiviz/zenhub-tasks#2605
# Part of the recidiviz-staging CJIS migration (#2335)

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

if [[ $# -lt 1 ]] || [[ "$1" == "--apply" ]]; then
    echo "Usage: $0 <project-id> [--apply]"
    echo ""
    echo "  project-id  GCP project to apply CMEK to (e.g. recidiviz-staging)"
    echo "  --apply     Actually make changes (default is dry-run)"
    exit 1
fi

PROJECT_ID="$1"
CMEK_PROJECT="cmek-82ade411-5705-4461-b2cb-9"
KEYRING_NAME="gcs-cmek"
ROTATION_PERIOD="7776000s"  # 90 days, matches TF-managed keys

# Look up the project number dynamically — needed to construct the GCS
# service agent email, which is service-{NUMBER}@gs-project-accounts.iam.
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
if [[ -z "$PROJECT_NUMBER" ]]; then
    echo "ERROR: Could not determine project number for ${PROJECT_ID}"
    exit 1
fi
GCS_SERVICE_AGENT="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"

# Parse remaining arguments
DRY_RUN=true
if [[ "${2:-}" == "--apply" ]]; then
    DRY_RUN=false
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if $DRY_RUN; then
    log "=== DRY RUN MODE — no changes will be made ==="
    log "=== Run with --apply to make changes ==="
    echo ""
fi

log "Listing all buckets in ${PROJECT_ID}..."
ALL_BUCKETS=$(gcloud storage ls --project="$PROJECT_ID" 2>/dev/null | sed 's|gs://||; s|/$||')
TOTAL=$(echo "$ALL_BUCKETS" | wc -l | tr -d ' ')
log "Found ${TOTAL} buckets"
echo ""

ALREADY_CMEK=0
APPLIED=0
FAILED=0

for bucket in $ALL_BUCKETS; do
    # Skip buckets that already have CMEK — check the default_kms_key field.
    existing_key=$(gcloud storage buckets describe "gs://${bucket}" \
        --format="value(default_kms_key)" 2>/dev/null || true)
    if [[ -n "$existing_key" ]]; then
        ALREADY_CMEK=$((ALREADY_CMEK + 1))
        continue
    fi

    log "Processing: ${bucket}"

    # Get the bucket's location (lowercased for KMS compatibility).
    location=$(gcloud storage buckets describe "gs://${bucket}" \
        --format="value(location)" 2>/dev/null | tr '[:upper:]' '[:lower:]')
    if [[ -z "$location" ]]; then
        log "  ERROR: Could not determine location — skipping"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Key name = bucket name, sanitized for KMS constraints.
    # KMS key names must be 1-63 chars, matching [a-zA-Z0-9_-]+.
    # Some bucket names have dots (e.g. appspot.com) — replace with hyphens.
    key_name=$(echo "$bucket" | tr '.' '-' | cut -c1-63)
    key_path="projects/${CMEK_PROJECT}/locations/${location}/keyRings/${KEYRING_NAME}/cryptoKeys/${key_name}"

    if $DRY_RUN; then
        log "  location=${location} key=${key_name}"
        log "  [DRY RUN] Would create keyring, key, IAM grant, and set encryption"
        APPLIED=$((APPLIED + 1))
        echo ""
        continue
    fi

    # Step 1: Create keyring (idempotent — no-ops if it already exists).
    if ! gcloud kms keyrings create "$KEYRING_NAME" \
        --location="$location" \
        --project="$CMEK_PROJECT" 2>/dev/null; then
        # "Already exists" is expected and fine; other errors will show.
        true
    fi

    # Step 2: Create per-bucket crypto key (idempotent — no-ops if it exists).
    NEXT_ROTATION=$(date -u -v+90d +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
        || date -u -d "+90 days" +"%Y-%m-%dT%H:%M:%SZ")
    if ! gcloud kms keys create "$key_name" \
        --keyring="$KEYRING_NAME" \
        --location="$location" \
        --project="$CMEK_PROJECT" \
        --purpose=encryption \
        --rotation-period="$ROTATION_PERIOD" \
        --next-rotation-time="$NEXT_ROTATION" \
        --protection-level=software 2>/dev/null; then
        # "Already exists" is expected and fine.
        true
    fi

    # Step 3: Grant the GCS service agent encrypt/decrypt on the key.
    # add-iam-policy-binding is idempotent — adding a duplicate binding is a no-op.
    if ! gcloud kms keys add-iam-policy-binding "$key_name" \
        --keyring="$KEYRING_NAME" \
        --location="$location" \
        --project="$CMEK_PROJECT" \
        --role="roles/cloudkms.cryptoKeyEncrypterDecrypter" \
        --member="serviceAccount:${GCS_SERVICE_AGENT}" \
        --quiet > /dev/null 2>&1; then
        log "  ERROR: Failed to grant IAM — skipping"
        FAILED=$((FAILED + 1))
        continue
    fi

    # Step 4: Set the key as the bucket's default encryption key.
    if ! gcloud storage buckets update "gs://${bucket}" \
        --default-encryption-key="$key_path" \
        --quiet 2>/dev/null; then
        log "  ERROR: Failed to set encryption — skipping"
        FAILED=$((FAILED + 1))
        continue
    fi

    APPLIED=$((APPLIED + 1))
    log "  Done"
    echo ""
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
log "========================================="
log "SUMMARY"
log "========================================="
log "Total buckets:    ${TOTAL}"
log "Already CMEK'd:   ${ALREADY_CMEK}"
log "Applied:          ${APPLIED}"
log "Failed:           ${FAILED}"
log "========================================="

if $DRY_RUN; then
    echo ""
    log "This was a DRY RUN. To apply changes, run:"
    log "  $0 $PROJECT_ID --apply"
fi

if [[ $FAILED -gt 0 ]]; then
    log "WARNING: ${FAILED} buckets failed — check output above for details"
    exit 1
fi
