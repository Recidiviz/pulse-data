# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

# Workload Identity Federation resources that let Edovo (an external partner)
# authenticate to our `POST /edovo/course-completions` endpoint from their AWS
# environment without long-lived shared secrets (OBT-23211). Edovo's AWS
# workload assumes their IAM role, exchanges that AWS credential for a GCP
# token through the pool below, and calls the endpoint as the `edovo-wif@`
# service account.
#
# Edovo runs several AWS environments, mapped onto our GCP projects per env (our
# staging trusts Edovo's test and staging roles; our prod trusts Edovo's
# production role). A GCP AWS provider is bound to a single AWS account, so we
# create one provider per distinct account and one workloadIdentityUser binding
# per role.
#
# The account/role principals are read from the `edovo_aws_principals` secret in
# each project (not committed to the repo, since they are a partner's AWS
# identifiers) — following the same Secret-Manager pattern as `iam_admin_panel_access`.
# The secret holds a YAML list of `{account_id, role_arn}` for that project;
# account_id must be quoted so leading zeros survive. Create/update it out of
# band with `gcloud secrets`.
#
# The `edovo-wif@` service account is deliberately granted NO project IAM roles:
# it exists purely as an identity to authenticate as. The endpoint (OBT-27565)
# authorizes callers by checking the verified identity/audience of the token,
# not GCP resource permissions.

data "google_secret_manager_secret_version" "edovo_aws_principals" {
  secret = "edovo_aws_principals"
}

locals {
  # This project's principals, from its Secret Manager secret.
  edovo_aws_principals = yamldecode(
    data.google_secret_manager_secret_version.edovo_aws_principals.secret_data
  )

  # Per role, the STS assumed-role ARN (`arn:aws:sts::<account>:assumed-role/<name>`)
  # that the `attribute.aws_role` claim arrives as during the token exchange —
  # derived from the IAM role ARN Edovo shares with us. Keyed by role_arn (unique).
  edovo_principals = {
    for p in local.edovo_aws_principals : p.role_arn => {
      account_id       = p.account_id
      assumed_role_arn = "arn:aws:sts::${p.account_id}:assumed-role/${regex("^arn:aws:iam::[0-9]{12}:role/([^/]+)$", p.role_arn)[0]}"
    }
  }

  # Distinct AWS accounts -> the assumed-role ARNs accepted from each, so every
  # provider is bound to one account and admits exactly its env's role(s).
  edovo_accounts = {
    for acct in distinct([for p in local.edovo_aws_principals : p.account_id]) :
    acct => [for role_arn, p in local.edovo_principals : p.assumed_role_arn if p.account_id == acct]
  }
}

resource "google_iam_workload_identity_pool" "edovo" {
  workload_identity_pool_id = "edovo-pool"
  display_name              = "Edovo"
  description               = <<EOT
Workload Identity Federation pool through which Edovo's AWS workloads
authenticate to Recidiviz (OBT-23211). Managed in Terraform.
EOT
}

resource "google_iam_workload_identity_pool_provider" "edovo_aws" {
  for_each = local.edovo_accounts

  workload_identity_pool_id          = google_iam_workload_identity_pool.edovo.workload_identity_pool_id
  workload_identity_pool_provider_id = "edovo-aws-${each.key}"
  display_name                       = "Edovo AWS ${each.key}"
  description                        = "AWS identity provider for Edovo account ${each.key} (OBT-23211). Managed in Terraform."

  aws {
    account_id = each.key
  }

  # Documented default attribute mappings for AWS providers
  # (https://cloud.google.com/iam/docs/workload-identity-federation#mapping),
  # declared explicitly so the attribute condition and IAM bindings can rely on
  # `attribute.aws_role`.
  attribute_mapping = {
    "google.subject"     = "assertion.arn"
    "attribute.aws_role" = "assertion.arn.contains('assumed-role') ? assertion.arn.extract('{account_arn}assumed-role/') + 'assumed-role/' + assertion.arn.extract('assumed-role/{role_name}/') : assertion.arn"
  }

  # Defense-in-depth: the workloadIdentityUser bindings below already scope
  # access to these exact roles, but also reject assertions from any other
  # principal in this account at the provider layer.
  attribute_condition = join(" || ", [for arn in each.value : "attribute.aws_role == \"${arn}\""])

  lifecycle {
    # Fail closed. An account with no roles would make attribute_condition an
    # empty string, which GCP treats as "no condition" — trusting the entire AWS
    # account. The grouping in locals can't currently produce that, but guard it
    # explicitly rather than rely on the invariant holding forever.
    precondition {
      condition     = length(each.value) > 0
      error_message = "Edovo account ${each.key} maps to no roles; an empty attribute_condition would trust the whole AWS account."
    }
  }
}

# Note: GCP requires service account IDs to be 6-30 characters, so the
# `edovo@` ID that OBT-23211 asks for is not possible; `edovo-wif@` is the
# closest compliant ID.
resource "google_service_account" "edovo" {
  account_id   = "edovo-wif"
  display_name = "Edovo WIF Service Account"
  description  = <<EOT
Identity that Edovo's AWS workloads federate to through the edovo-pool
Workload Identity Federation pool (OBT-23211). Deliberately granted no
project IAM roles. Managed in Terraform.
EOT
}

resource "google_service_account_iam_member" "edovo_workload_identity_user" {
  for_each = local.edovo_principals

  service_account_id = google_service_account.edovo.name
  role               = "roles/iam.workloadIdentityUser"
  # Scoped to each specific AWS role via `attribute.aws_role`, not the whole
  # AWS account.
  member = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.edovo.name}/attribute.aws_role/${each.value.assumed_role_arn}"

  lifecycle {
    # Validate the secret's values at plan time (they aren't in the repo to be
    # reviewed): a 12-digit account and a role ARN that belongs to it.
    precondition {
      condition = (
        can(regex("^[0-9]{12}$", each.value.account_id)) &&
        startswith(each.key, "arn:aws:iam::${each.value.account_id}:role/")
      )
      error_message = "Edovo principal [${each.key}] must be a role ARN in 12-digit account [${each.value.account_id}]."
    }
  }
}

# Everything Edovo needs to configure their side of the federation handshake.
# The token-exchange audience for a given AWS account is that account's provider
# full name; Edovo uses the audience matching the account their workload runs in.
output "edovo_workload_identity_pool_name" {
  value = google_iam_workload_identity_pool.edovo.name
}

output "edovo_service_account_email" {
  value = google_service_account.edovo.email
}

output "edovo_workload_identity_provider_names_by_account" {
  value = {
    for acct, provider in google_iam_workload_identity_pool_provider.edovo_aws :
    acct => provider.name
  }
}
