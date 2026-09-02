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
- Deny policy `catastrophic-delete-guardrail` — denies `bigquery.datasets.delete` **and
  `bigquery.tables.delete`** on protection-tagged datasets to all workforce humans.
  Resource Manager tags inherit from a dataset to its tables, so tagging the dataset
  protects everything in it without per-table bindings.
- Exempt from that deny: the `breakglass@` group, and the two deploy service accounts
  (`cloud-build-ci-cd@recidiviz-123`, `cloud-build-ci-cd@recidiviz-staging`), which are
  named explicitly so `terraform apply` can retire an emptied dataset. Do not rely on
  service accounts being outside `principalSet://goog/cloudIdentityCustomerId/...` —
  Google does not document whether that set covers them, so the exemption is listed
  rather than assumed. Other service-account work (Airflow cleanup, view churn) targets
  untagged datasets and is unaffected either way.

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
`deny_policy.json` ships with only the tag-gated BigQuery rule, so the first
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
The deny blocks both `datasets.delete` and `tables.delete` for workforce humans, so emptying a
protected dataset is itself gated. The deploy service accounts are exempt, so the shape of the
retirement is still "empty it, then let the deploy drop the shell" — but step 2 needs elevation.

1. **In a PR**, remove the dataset from the protect set — from `datasets_to_protect()` (its
   source-table registry tier, or the `INFRA_PROTECT_ALLOWLIST`) and from the Terraform-managed
   dataset registry. Note the reconcile job is **add-only**: this stops it re-tagging the
   dataset but does not remove the tag already there. The lingering tag does not block the
   delete — the deploy SA is exempt — so there is no need to clear it by hand. Do not try:
   a BigQuery `PATCH` of `{"resourceTags": {}}` silently does nothing.
2. **Empty the dataset.** Deleting its tables needs both halves:
   - an *allow* for `bigquery.tables.delete`. The standing `recidivizengineeringrole` does
     **not** grant it (it grants `datasets.delete` but not `tables.delete`), and neither does
     dataset-level access on a platform-owned dataset. Request the **`pam-update-data`** lane,
     whose `roles/bigquery.dataEditor` supplies it — in staging as well as prod.
   - an exception to the *deny*: join `breakglass@`. Allow and deny are evaluated
     independently, so holding one without the other still fails. Group membership takes
     1–2 minutes to propagate; an immediate retry looks like break-glass not working.
3. **Merge the PR, then deploy.** `terraform apply` runs in Cloud Build as
   `cloud-build-ci-cd@<project>`, an explicit exception, so it deletes the now-empty dataset.
   Merge before deleting the dataset by hand — while Terraform still manages it, the next
   apply just recreates it. The shared dataset module sets `delete_contents_on_destroy = false`,
   so a destroy fails rather than dropping data if the dataset is not actually empty.

**Sequencing warning.** Emptying the dataset before the PR merges leaves a window where a
YAML config in `source_tables/externally_managed` declares a table that no longer exists in
BigQuery, which fails the **BQ Source Table Validation** check on every branch until the PR
lands. Keep that window short, and never open it just before a cherry-pick deploy.

## Status
**Live and enforcing on both `recidiviz-staging` and `recidiviz-123`.** Verified 2026-09-02:
the policy is attached to both projects, denies `datasets.delete` and `tables.delete`, and
249 of 730 prod datasets carry the tag. Real denials have been observed in prod, and
`breakglass@` membership has been confirmed to clear them.

The `tables.delete` permission was added to the live policy on 2026-09-01; the deploy
service-account exceptions on 2026-09-02. Both are reflected in `deny_policy.json` here.

Because the script is applied by hand rather than by the deploy pipeline, this file can
drift from what is live. Before trusting it, diff against the real thing:

```bash
# gcloud has no `iam policies describe`; read it over REST.
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "x-goog-user-project: recidiviz-security" \
  "https://iam.googleapis.com/v2/policies/cloudresourcemanager.googleapis.com%2Fprojects%2F<PROJECT_NUMBER>/denypolicies/catastrophic-delete-guardrail"
```

Design origin: validated end-to-end in `recidiviz-terraform-sandbox` (2026-08-14), where a
tag-conditioned `datasets.delete` deny fired on a tagged dataset and overrode `bigquery.admin`.
