#!/usr/bin/env bash
#
# Creates (or updates) the catastrophic-delete deny policy and the `protection` tag it is
# conditioned on -- the deletion-protection guardrail for BigQuery/GCS/KMS. BigQuery and GCS
# have no native delete protection, so an IAM deny policy is the only thing that stops a
# role-holder (even an owner) from dropping a dataset or bucket.
#
# Applied MANUALLY by Aurora -- NOT the deploy pipeline (these are org-scoped, rarely-changing
# resources). Idempotent: safe to re-run. See README.md for the runbook and rollout phases.
#
# Usage (staging soak, the default):
#   CUSTOMER_ID="$(gcloud organizations describe 448885369991 \
#     --format='value(directoryCustomerId)')" \
#     ./create_deletion_protection_guardrail.sh
#
# Org rollout: also set
#   ATTACHMENT_POINT="cloudresourcemanager.googleapis.com/organizations/448885369991"
set -euo pipefail

ORG_ID="448885369991"
TAG_KEY_SHORT="protection"
TAG_VALUE_SHORT="managed-data"
POLICY_ID="catastrophic-delete-guardrail"
BREAKGLASS_GROUP="${BREAKGLASS_GROUP:-breakglass@recidiviz.org}"
# Staging by default so the first apply is contained.
ATTACHMENT_POINT="${ATTACHMENT_POINT:-cloudresourcemanager.googleapis.com/projects/recidiviz-staging}"
CUSTOMER_ID="${CUSTOMER_ID:-}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${CUSTOMER_ID}" ]]; then
  echo "ERROR: set CUSTOMER_ID (the Cloud Identity customer id). Look it up with:" >&2
  echo "  gcloud organizations describe ${ORG_ID} --format='value(directoryCustomerId)'" >&2
  exit 1
fi

echo "== 1/3  ensure org tag [${TAG_KEY_SHORT}=${TAG_VALUE_SHORT}] =="
key_name="$(gcloud resource-manager tags keys list --parent="organizations/${ORG_ID}" \
  --filter="shortName=${TAG_KEY_SHORT}" --format="value(name)" 2>/dev/null || true)"
if [[ -z "${key_name}" ]]; then
  gcloud resource-manager tags keys create "${TAG_KEY_SHORT}" \
    --parent="organizations/${ORG_ID}" \
    --description="Marks resources holding platform-managed persisted data that workforce humans may not delete."
  key_name="$(gcloud resource-manager tags keys list --parent="organizations/${ORG_ID}" \
    --filter="shortName=${TAG_KEY_SHORT}" --format="value(name)")"
fi
value_name="$(gcloud resource-manager tags values list --parent="${key_name}" \
  --filter="shortName=${TAG_VALUE_SHORT}" --format="value(name)" 2>/dev/null || true)"
if [[ -z "${value_name}" ]]; then
  gcloud resource-manager tags values create "${TAG_VALUE_SHORT}" --parent="${key_name}" \
    --description="Persisted platform-managed data (BigQuery source-table registry). Delete-protected."
  value_name="$(gcloud resource-manager tags values list --parent="${key_name}" \
    --filter="shortName=${TAG_VALUE_SHORT}" --format="value(name)")"
fi
echo "   tag key=[${key_name}] value=[${value_name}]"

echo "== 2/3  render deny policy from deny_policy.json =="
rendered="$(mktemp)"
trap 'rm -f "${rendered}"' EXIT
sed -e "s|CUSTOMER_ID_PLACEHOLDER|${CUSTOMER_ID}|g" \
  -e "s|BREAKGLASS_GROUP_PLACEHOLDER|${BREAKGLASS_GROUP}|g" \
  -e "s|TAG_KEY_ID_PLACEHOLDER|${key_name}|g" \
  -e "s|TAG_VALUE_ID_PLACEHOLDER|${value_name}|g" \
  "${script_dir}/deny_policy.json" >"${rendered}"

echo "== 3/3  create-or-update deny policy [${POLICY_ID}] on [${ATTACHMENT_POINT}] =="
if gcloud iam policies list --attachment-point="${ATTACHMENT_POINT}" --kind=denypolicies \
  --format="value(name)" 2>/dev/null | grep -q "/${POLICY_ID}$"; then
  echo "   policy exists -- updating"
  gcloud iam policies update "${POLICY_ID}" --attachment-point="${ATTACHMENT_POINT}" \
    --kind=denypolicies --policy-file="${rendered}"
else
  echo "   creating policy"
  gcloud iam policies create "${POLICY_ID}" --attachment-point="${ATTACHMENT_POINT}" \
    --kind=denypolicies --policy-file="${rendered}"
fi
echo "== done =="
