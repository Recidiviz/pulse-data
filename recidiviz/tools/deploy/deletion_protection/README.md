# Deletion-protection guardrail

Prevents catastrophic deletion of persisted data. BigQuery and GCS have **no native delete
protection** (unlike Cloud SQL's `deletion_protection_enabled` and KMS's `prevent_destroy`),
so an IAM **deny policy** is the only thing that stops a role-holder — even an `owner` — from
dropping a dataset or bucket.

## Why gcloud and not Terraform
These are **org-scoped, rarely-changing** resources (one tag + one deny policy) applied
**manually by Aurora**, outside the per-project deploy pipeline. A standalone Terraform root
would add its own state bucket, provider pins, and state-vs-reality drift while getting *none*
of Terraform's usual payoff (it isn't CI-planned or pipeline-applied). A committed, idempotent
gcloud script is version-controlled, reviewable, and re-runnable with far less to babysit.

Drift detection — Terraform's one real advantage here — is instead covered by a **log-based
alert on any `denypolicies` mutation** (real-time tamper detection, stronger than waiting for
the next plan). Long term, if we want these managed declaratively, the right home is the
`security-operations-automation` Terraform alongside the PAM resources — not a one-off root.

## What it creates
- Org tag `protection = managed-data` — a separate reconcile job
  ([`apply_dataset_protection_tags_entrypoint.py`](../../../entrypoints/bigquery/apply_dataset_protection_tags_entrypoint.py))
  stamps it onto the protected-tier source tables.
- Deny policy `catastrophic-delete-guardrail` — denies `bigquery.datasets.delete` on
  protection-tagged datasets to all workforce humans; `breakglass@` exempt; service accounts
  excluded (so the deploy, Airflow cleanup, and view churn keep working).

## Prerequisites
1. **Cloud Identity customer id** — `gcloud organizations describe 448885369991 --format='value(directoryCustomerId)'`.
2. **`breakglass@recidiviz.org`** group exists and is empty (Aurora manages membership).
3. Applier holds org `resourcemanager.tagAdmin` + `iam.denyAdmin` on the attachment point.

## Run it
```bash
# Staging soak (default attachment point):
CUSTOMER_ID="$(gcloud organizations describe 448885369991 --format='value(directoryCustomerId)')" \
  ./create_deletion_protection_guardrail.sh

# Org rollout: also set
ATTACHMENT_POINT="cloudresourcemanager.googleapis.com/organizations/448885369991"
```
The script is idempotent — it creates the tag/policy if absent and updates the policy if it
already exists.

## Rollout
1. **Dry-run** — before enforcing, run the [IAM Policy Simulator deny simulation](https://cloud.google.com/iam/docs/deny-simulator) against the target scope; it replays 90 days of access and lists who would newly be denied.
2. **Staging soak** — run as-is (attaches to `recidiviz-staging`), run the reconcile job to tag staging's protect set, confirm a hand delete of a tagged dataset returns a 403. Soak ~1 week.
3. **Org rollout** — set `ATTACHMENT_POINT` to the organization and re-run.

## Adding the blanket bucket / KMS rules (after the dry-run)
`deny_policy.json` ships with only the tag-gated `bigquery.datasets.delete` rule, so the first
apply is safe. Once their dry-run is clean, add these two rule objects to the `rules` array and
re-run the script (they are **blanket** — no tag condition — so they take effect immediately):

```json
{
  "denyRule": {
    "deniedPrincipals": ["principalSet://goog/cloudIdentityCustomerId/CUSTOMER_ID_PLACEHOLDER"],
    "exceptionPrincipals": ["principalSet://goog/group/BREAKGLASS_GROUP_PLACEHOLDER"],
    "deniedPermissions": ["storage.googleapis.com/buckets.delete"]
  }
},
{
  "denyRule": {
    "deniedPrincipals": ["principalSet://goog/cloudIdentityCustomerId/CUSTOMER_ID_PLACEHOLDER"],
    "exceptionPrincipals": ["principalSet://goog/group/BREAKGLASS_GROUP_PLACEHOLDER"],
    "deniedPermissions": ["cloudkms.googleapis.com/cryptoKeyVersions.destroy"]
  }
}
```

## Deleting a protected dataset
The deny only blocks `bigquery.datasets.delete`, and only for workforce humans — service accounts
are exempt (see "What it creates"). So a protected dataset is retired by emptying it by hand and
letting the deploy service account drop the empty shell:

1. **In a PR**, remove the dataset from the protect set — from `datasets_to_protect()` (its
   source-table registry tier, or the `INFRA_PROTECT_ALLOWLIST`) and from the Terraform-managed
   dataset registry. The reconcile job stops re-tagging it; once Terraform owns the tag it also
   clears `protection` on the next apply. (The lingering tag doesn't block the delete either way —
   the deploy SA is exempt from the deny — but clearing it keeps the tag set honest.)
2. **Empty the dataset by hand** — delete its tables and views in BigQuery. The deny covers only
   `datasets.delete`, not table deletes, so this is allowed; the dataset itself still can't be
   deleted by a human while it's tagged.
3. **Deploy the PR.** `terraform apply` runs as the deploy/Compute service account, which the deny
   policy does not target, so it deletes the now-empty dataset.

## Status
Design validated end-to-end in `recidiviz-terraform-sandbox` (2026-08-14): a tag-conditioned
`datasets.delete` deny fired on a tagged BQ dataset and overrode `bigquery.admin`. **Not applied
to any real project yet.**
