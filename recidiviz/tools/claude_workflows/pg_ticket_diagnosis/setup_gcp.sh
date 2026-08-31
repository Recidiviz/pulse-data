#!/bin/bash
# Provisions the secret VALUES for the PG diagnosis pipeline. Safe to re-run.
#
# Everything else (service account, IAM roles, secret containers, secret-level
# access, BigQuery row-access-policy group memberships, Cloud Build trigger)
# is defined in Terraform: recidiviz/tools/deploy/terraform/pg-diagnosis.tf.
# Run this script after that Terraform has been applied — the secret
# containers must exist before values can be added to them.
#
# Secret values are deliberately not stored in Terraform: they would end up in
# plain text in the Terraform state bucket.

# TODO(#70351): Switch to recidiviz-123 after getting prod SA permissions
PROJECT_ID="recidiviz-staging"

gcloud config set project "$PROJECT_ID"

secret_has_version() {
  local secret_name="$1"
  gcloud secrets versions list "$secret_name" --project="$PROJECT_ID" \
    --format="value(name)" 2>/dev/null | grep -q .
}

# 1. The Cloud Build webhook auth secret. The trigger
# (pg-diagnosis.tf) references version 1 of this secret to authenticate
# webhook calls. The value is random and untyped — anything fits, as long as
# the same value is used in the webhook URL that pg-diagnosis.yml POSTs to.
echo "==> Provisioning Cloud Build webhook secret value..."
WEBHOOK_SECRET_NAME="github_pg_diagnosis_webhook"
if secret_has_version "$WEBHOOK_SECRET_NAME"; then
  echo "    $WEBHOOK_SECRET_NAME already has a version, skipping."
else
  # 32 random bytes (64 hex chars) is plenty of entropy.
  if openssl rand -hex 32 | tr -d '\n' \
    | gcloud secrets versions add "$WEBHOOK_SECRET_NAME" --data-file=- --project="$PROJECT_ID"; then
    echo "    Added."
  else
    echo "    FAILED — has pg-diagnosis.tf been applied to $PROJECT_ID yet?"
  fi
fi

# 2. The Anthropic API key for the agent loop.
echo "==> Provisioning Anthropic API key..."
SECRET_NAME="pg_diagnosis_claude_api_key"
if secret_has_version "$SECRET_NAME"; then
  echo "    $SECRET_NAME already has a version."
  read -rp "    Add a new version? (y/N): " REGEN
  if [[ ! "$REGEN" =~ ^[Yy]$ ]]; then
    echo "    Skipping."
    REGEN=""
  fi
else
  REGEN="y"
fi
if [[ "$REGEN" =~ ^[Yy]$ ]]; then
  echo "    Paste your Anthropic API key, then press Enter:"
  read -rs SECRET_VALUE
  if echo -n "$SECRET_VALUE" | gcloud secrets versions add "$SECRET_NAME" --data-file=- --project="$PROJECT_ID"; then
    echo "    Stored."
  else
    echo "    FAILED — has pg-diagnosis.tf been applied to $PROJECT_ID yet?"
  fi
fi

# 3. Summary. The remaining secrets the agent reads
# (github_deploy_script_pat, linear_deploy_script_api_key) are owned and
# provisioned by the deploy scripts; pg-diagnosis.tf only grants the agent SA
# access to them. The Linear key must be a read-capable Linear API key — to
# create a staging copy of the prod linear_deploy_script_api_key secret:
#   gcloud secrets versions access latest --secret=linear_deploy_script_api_key \
#     --project=recidiviz-123 \
#     | gcloud secrets create linear_deploy_script_api_key --data-file=- \
#       --project="$PROJECT_ID" --replication-policy=user-managed --locations=us-west1
echo ""
echo "============================================================"
echo "Secret values provisioned."
echo ""
echo "The GitHub Action workflow (.github/workflows/pg-diagnosis.yml)"
echo "POSTs to a Cloud Build webhook trigger declared in"
echo "recidiviz/tools/deploy/terraform/pg-diagnosis.tf, which"
echo "runs the build steps in"
echo "recidiviz/tools/claude_workflows/pg_ticket_diagnosis/cloudbuild.yaml."
echo ""
echo "Remaining one-time setup (do after the next staging deploy applies"
echo "pg-diagnosis.tf):"
echo "  1. Grab the trigger's webhook URL from the Cloud Build UI"
echo "     (it embeds the github_pg_diagnosis_webhook secret value)."
echo "  2. Set it as the CLOUD_BUILD_PG_DIAGNOSIS_WEBHOOK secret in"
echo "     GitHub Actions (repo settings → Secrets and variables)."
echo "============================================================"
