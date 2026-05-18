#!/bin/bash
# Run this once to set up GCP resources for the PG diagnosis Cloud Build job.
# Safe to re-run — skips resources that already exist.

# TODO(#70351): Switch to recidiviz-123 after getting prod SA permissions
PROJECT_ID="recidiviz-staging"
SA_NAME="diagnosis-for-pg-ticket"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

gcloud config set project "$PROJECT_ID"

has_role() {
  local sa_email="$1" role="$2"
  gcloud projects get-iam-policy "$PROJECT_ID" \
    --flatten="bindings[].members" \
    --filter="bindings.members:$sa_email AND bindings.role:$role" \
    --format="value(bindings.role)" 2>/dev/null | grep -q .
}

grant_role() {
  local sa_email="$1" role="$2"
  if has_role "$sa_email" "$role"; then
    echo "    $role — already bound, skipping."
    return
  fi
  echo "    Binding $role to $sa_email..."
  if gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$sa_email" \
    --role="$role" \
    --condition=None \
    --quiet > /dev/null 2>&1; then
    echo "    OK."
  else
    echo "    FAILED — you may need Security Admin permissions."
  fi
}


# 1. Enable required APIs
echo "==> Enabling APIs..."
gcloud services enable \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  bigquery.googleapis.com \
  docs.googleapis.com \
  artifactregistry.googleapis.com
echo "    Done."

# 2. Create a dedicated service account (skip if exists)
echo "==> Creating service account..."
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" > /dev/null 2>&1; then
  echo "    Already exists, skipping."
else
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="Agent performing initial diagnosis and triage of incoming Product Growth tickets"
  echo "    Created."
fi

# 3. Create Artifact Registry repo for Docker image caching (skip if exists)
echo "==> Creating Artifact Registry repo..."
if gcloud artifacts repositories describe pg-diagnosis \
  --location=us-central1 --project="$PROJECT_ID" > /dev/null 2>&1; then
  echo "    Already exists, skipping."
else
  gcloud artifacts repositories create pg-diagnosis \
    --repository-format=docker \
    --location=us-central1 \
    --project="$PROJECT_ID" \
    --description="Docker images for PG ticket diagnosis agent"
  echo "    Created."
fi

# 4. Grant IAM roles to the service account.
# The Cloud Build trigger defined in pg-diagnosis-trigger.tf runs as this SA;
# these are the runtime permissions it needs.
echo "==> Granting IAM roles..."
grant_role "$SA_EMAIL" "roles/cloudbuild.builds.editor"
grant_role "$SA_EMAIL" "roles/bigquery.dataViewer"
grant_role "$SA_EMAIL" "roles/bigquery.jobUser"
grant_role "$SA_EMAIL" "roles/logging.logWriter"
grant_role "$SA_EMAIL" "roles/artifactregistry.writer"

# 5. Grant the SA permission to mint OAuth tokens for itself.
# The agent's PII-doc fetch (fetch_pii_for_issue in run_pg_ticket_diagnosis.py)
# self-impersonates to get a token scoped to documents.readonly, which the
# default Cloud Build credentials don't have. `add-iam-policy-binding` is
# naturally idempotent so no skip-guard is needed.
echo "==> Granting self-impersonation binding..."
if gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/iam.serviceAccountTokenCreator" \
  --quiet > /dev/null 2>&1; then
  echo "    OK."
else
  echo "    FAILED — you may need Service Account Admin permissions."
fi

# 6. Grant BigQuery row-access-policy group memberships.
# `normalized_state.state_person_external_id` and other downstream tables
# have row-access policies that filter by state_code. Project-level
# `bigquery.dataViewer` alone is not enough — the SA must also be a member of
# the per-state grantee groups, otherwise queries silently return 0 rows.
# `s-me-data` (US_ME) is intentionally excluded; we do not access Maine data.
#
# This uses `gcloud identity groups memberships add`, which requires the caller
# to have Google Workspace groups-admin permissions. If a grant fails, add the
# SA manually at https://admin.google.com/ac/groups.
add_to_group() {
  local group_email="$1" sa_email="$2"
  if gcloud identity groups memberships list \
    --group-email="$group_email" \
    --format="value(preferredMemberKey.id)" 2>/dev/null | grep -qx "$sa_email"; then
    echo "    $group_email — already a member, skipping."
    return
  fi
  echo "    Adding $sa_email to $group_email..."
  if gcloud identity groups memberships add \
    --group-email="$group_email" \
    --member-email="$sa_email" \
    --quiet > /dev/null 2>&1; then
    echo "    OK."
  else
    echo "    FAILED — add manually at https://admin.google.com/ac/groups (needs groups-admin)."
  fi
}

echo "==> Granting BigQuery row-access-policy group memberships..."
for GROUP in \
  s-default-state-data@recidiviz.org \
  s-az-data@recidiviz.org \
  s-id-data@recidiviz.org \
  s-ix-data@recidiviz.org \
  s-mi-data@recidiviz.org \
  s-nc-data@recidiviz.org \
  s-pa-data@recidiviz.org \
  s-ut-data@recidiviz.org; do
  add_to_group "$GROUP" "$SA_EMAIL"
done

# 7. Create the Cloud Build webhook auth secret.
# Used by pg-diagnosis-trigger.tf to authenticate webhook calls. The value is
# random and untyped — anything fits, as long as the same value is used in the
# webhook URL that pg-diagnosis.yml POSTs to.
echo "==> Creating Cloud Build webhook secret..."
WEBHOOK_SECRET_NAME="github_pg_diagnosis_webhook"
if gcloud secrets describe "$WEBHOOK_SECRET_NAME" --project="$PROJECT_ID" > /dev/null 2>&1; then
  echo "    $WEBHOOK_SECRET_NAME already exists, skipping."
else
  # 32 random bytes (64 hex chars) is plenty of entropy.
  WEBHOOK_SECRET_VALUE=$(openssl rand -hex 32)
  if echo -n "$WEBHOOK_SECRET_VALUE" | gcloud secrets create "$WEBHOOK_SECRET_NAME" \
    --data-file=- --project="$PROJECT_ID" \
    --replication-policy=user-managed --locations=us-west1; then
    echo "    Created."
  else
    echo "    FAILED to create $WEBHOOK_SECRET_NAME."
  fi
fi

# 8. Grant secret-level access (required for Cloud Build availableSecrets)
echo "==> Granting secret-level access..."
for SECRET in pg_diagnosis_claude_api_key github_deploy_script_pat; do
  echo "    Granting access to $SECRET..."
  if gcloud secrets add-iam-policy-binding "$SECRET" \
    --project="$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/secretmanager.secretAccessor" \
    --quiet > /dev/null 2>&1; then
    echo "    OK."
  else
    echo "    FAILED — you may need Security Admin permissions."
  fi
done

# 9. Store Anthropic API key (GitHub token uses existing github_deploy_script_pat secret)
echo "==> Storing secrets..."
SECRET_NAME="pg_diagnosis_claude_api_key"
if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" > /dev/null 2>&1; then
  echo "    $SECRET_NAME already exists."
  read -rp "    Regenerate? (y/N): " REGEN
  if [[ "$REGEN" =~ ^[Yy]$ ]]; then
    echo "    Paste your Anthropic API key, then press Enter:"
    read -rs SECRET_VALUE
    if echo -n "$SECRET_VALUE" | gcloud secrets versions add "$SECRET_NAME" --data-file=- --project="$PROJECT_ID"; then
      echo "    Updated."
    else
      echo "    FAILED to update."
    fi
  else
    echo "    Skipping."
  fi
else
  echo "    Paste your Anthropic API key, then press Enter:"
  read -rs SECRET_VALUE
  if echo -n "$SECRET_VALUE" | gcloud secrets create "$SECRET_NAME" --data-file=- --project="$PROJECT_ID" --replication-policy=user-managed --locations=us-west1; then
    echo "    Stored."
  else
    echo "    FAILED to store."
  fi
fi

# 10. Summary
echo ""
echo "============================================================"
echo "Setup complete."
echo ""
echo "The GitHub Action workflow (.github/workflows/pg-diagnosis.yml)"
echo "POSTs to a Cloud Build webhook trigger declared in"
echo "recidiviz/tools/deploy/terraform/pg-diagnosis-trigger.tf, which"
echo "runs the build steps in"
echo "recidiviz/tools/claude_workflows/pg_ticket_diagnosis/cloudbuild.yaml."
echo ""
echo "Remaining one-time setup (do after the next staging deploy applies"
echo "pg-diagnosis-trigger.tf):"
echo "  1. Grab the trigger's webhook URL from the Cloud Build UI"
echo "     (it embeds the github_pg_diagnosis_webhook secret value)."
echo "  2. Set it as the CLOUD_BUILD_PG_DIAGNOSIS_WEBHOOK secret in"
echo "     GitHub Actions (repo settings → Secrets and variables)."
echo "============================================================"
