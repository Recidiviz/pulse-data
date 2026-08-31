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
# Infrastructure for the PG ticket diagnosis pipeline: the service account the
# agent runs as, its IAM grants, the secrets it reads, its BigQuery
# row-access-policy group memberships, and the Cloud Build webhook trigger
# that runs it. Secret *values* are provisioned by
# recidiviz/tools/claude_workflows/pg_ticket_diagnosis/setup_gcp.sh.

locals {
  # TODO(#70351): Enable in recidiviz-123 after getting prod SA permissions.
  pg_diagnosis_enabled = var.project_id == "recidiviz-staging"

  pg_diagnosis_sa_email = google_service_account.pg_diagnosis.email

  # Runtime roles for the trigger's build, which runs as the PG diagnosis SA.
  pg_diagnosis_runtime_roles = [
    # Pulls the private appengine/default image the run step executes in.
    "roles/artifactregistry.reader",
    "roles/bigquery.dataViewer",
    "roles/bigquery.jobUser",
    "roles/cloudbuild.builds.editor",
    "roles/logging.logWriter",
  ]

  # `normalized_state.state_person_external_id` and other downstream tables
  # have row-access policies that filter by state_code. Project-level
  # `bigquery.dataViewer` alone is not enough — the SA must also be a member
  # of the per-state grantee groups, otherwise queries silently return 0 rows.
  # US_ME and US_CA are intentionally excluded; we do not access their data.
  pg_diagnosis_data_access_states = ["US_AZ", "US_ID", "US_IX", "US_MI", "US_NC", "US_PA", "US_UT"]
  pg_diagnosis_data_access_groups = merge(
    {
      for state in local.pg_diagnosis_data_access_states :
      state => local.state_data_access_group_resource_names[state]
    },
    # s-default-state-data@recidiviz.org, for non-restricted rows in
    # state-agnostic BQ tables.
    { DEFAULT = local.default_state_data_group_resource_name },
  )
}

# Google Docs API, used by the agent's PII-doc fetch (fetch_pii_for_issue in
# run_pg_ticket_diagnosis.py).
resource "google_project_service" "docs_api" {
  service = "docs.googleapis.com"

  # Other consumers may rely on this API; never disable it on destroy.
  disable_on_destroy = false
}

resource "google_service_account" "pg_diagnosis" {
  account_id   = "diagnosis-for-pg-ticket"
  display_name = "Agent performing initial diagnosis and triage of incoming Product Growth tickets"
}

resource "google_project_iam_member" "pg_diagnosis_runtime_iam" {
  for_each = local.pg_diagnosis_enabled ? toset(local.pg_diagnosis_runtime_roles) : toset([])
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${local.pg_diagnosis_sa_email}"
}

# The agent's PII-doc fetch self-impersonates to get a token scoped to
# documents.readonly, which the default Cloud Build credentials don't have.
#
# NOTE: this binding only gets the SA a scoped token — it does not grant
# access to any document. Per-ticket PII docs are created across SEVERAL Drive
# folders (e.g. 1alKihL5iNsXtG62NyKT6IfJC4k08whN5 and
# 1kjTBfySzQ5ZZkMV1Kn5emhe81K-GQo8j), and the SA must hold at least Viewer on
# each one for fetch_pii to work; per-doc access is inherited from the folder.
# Drive ACLs are not manageable in Terraform, so when the doc automation
# starts writing to a folder the SA isn't shared on, every diagnosis of a
# ticket landing there fails with PIIFetchError. Granting the SA access at
# the shared-drive level is the durable fix.
resource "google_service_account_iam_member" "pg_diagnosis_self_impersonation" {
  count              = local.pg_diagnosis_enabled ? 1 : 0
  service_account_id = google_service_account.pg_diagnosis.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${local.pg_diagnosis_sa_email}"
}

# Anthropic API key for the agent loop. The value is provisioned manually via
# setup_gcp.sh; Terraform manages only the secret container.
resource "google_secret_manager_secret" "pg_diagnosis_claude_api_key" {
  count     = local.pg_diagnosis_enabled ? 1 : 0
  secret_id = "pg_diagnosis_claude_api_key"
  replication {
    user_managed {
      replicas {
        location = "us-west1"
      }
    }
  }
}

# Auth secret for the Cloud Build webhook trigger below. The value is random
# and untyped — anything fits, as long as the same value is used in the
# webhook URL that .github/workflows/pg-diagnosis.yml POSTs to. Provisioned
# manually via setup_gcp.sh; Terraform manages only the secret container.
resource "google_secret_manager_secret" "github_pg_diagnosis_webhook" {
  count     = local.pg_diagnosis_enabled ? 1 : 0
  secret_id = "github_pg_diagnosis_webhook"
  replication {
    user_managed {
      replicas {
        location = "us-west1"
      }
    }
  }
}

# Secret-level access for the secrets the agent reads at runtime:
# pg_diagnosis_claude_api_key via build env injection, and
# github_deploy_script_pat (helperbot comments) / linear_deploy_script_api_key
# (Linear ID resolution) directly from Secret Manager via get_secret. The
# latter two secrets are owned by the deploy scripts; only our SA's membership
# is managed here.
resource "google_secret_manager_secret_iam_member" "pg_diagnosis_secret_access" {
  for_each = local.pg_diagnosis_enabled ? toset([
    "pg_diagnosis_claude_api_key",
    "github_deploy_script_pat",
    "linear_deploy_script_api_key",
  ]) : toset([])
  secret_id = each.key
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.pg_diagnosis_sa_email}"

  depends_on = [google_secret_manager_secret.pg_diagnosis_claude_api_key]
}

# The Cloud Build service agent resolves the trigger's webhook secret when a
# webhook call arrives, so it (not the build SA) needs access to it.
resource "google_secret_manager_secret_iam_member" "pg_diagnosis_webhook_cloudbuild_agent" {
  count     = local.pg_diagnosis_enabled ? 1 : 0
  secret_id = google_secret_manager_secret.github_pg_diagnosis_webhook[0].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
}

resource "google_cloud_identity_group_membership" "pg_diagnosis_state_data_group" {
  for_each = local.pg_diagnosis_enabled ? local.pg_diagnosis_data_access_groups : {}
  group    = each.value

  preferred_member_key {
    id = local.pg_diagnosis_sa_email
  }

  roles {
    name = "MEMBER"
  }
}

# Build steps live in cloudbuild.yaml and are inlined into the trigger's
# `build` block below. A webhook trigger cannot resolve its build config from
# a repo file at runtime (`filename`/`git_file_source` + `source_to_build`
# is rejected with INVALID_ARGUMENT); the steps must be baked into the trigger,
# exactly as terraform-plan-pr-commenter.tf does.
locals {
  pg_diagnosis_build = yamldecode(file("${path.module}/../../claude_workflows/pg_ticket_diagnosis/cloudbuild.yaml"))
}

# Trigger fired by GitHub Actions webhook (.github/workflows/pg-diagnosis.yml)
# when a new Product Growth ticket arrives. Runs the agent that diagnoses the
# issue and comments back. Replaces the prior architecture, which authenticated
# the GitHub Action via Workload Identity Federation to impersonate the SA
# and `gcloud builds submit` directly — opaque webhook separation removes the
# WIF impersonation path. The webhook secret protects against unauthorized
# invocations.
resource "google_cloudbuild_trigger" "pg_diagnosis" {
  provider = google-beta
  count    = local.pg_diagnosis_enabled ? 1 : 0
  name     = "pg-diagnosis"
  # Triggers that reference a 2nd-gen repository resource (see source_to_build
  # below) must live in the same region as the repository connection; they
  # cannot be created in the default "global" region.
  location    = "us-west1"
  description = "Diagnoses incoming Product Growth issues and comments on them"
  # The SA is set here (not in cloudbuild.yaml) — specifying it in both the
  # trigger and the build config is rejected.
  service_account = google_service_account.pg_diagnosis.name

  webhook_config {
    secret = "projects/${data.google_project.current.number}/secrets/${google_secret_manager_secret.github_pg_diagnosis_webhook[0].secret_id}/versions/1"
  }

  source_to_build {
    repository = "projects/${var.project_id}/locations/us-west1/connections/Github/repositories/Recidiviz-pulse-data"
    ref        = "refs/heads/main"
    repo_type  = "GITHUB"
  }

  # Webhook-supplied values, mapped onto the build's substitutions.
  substitutions = {
    _ISSUE_NUMBER  = "$(body.ISSUE_NUMBER)"
    _ISSUE_TITLE   = "$(body.ISSUE_TITLE)"
    _ISSUE_BODY    = "$(body.ISSUE_BODY)"
    _ISSUE_REPO    = "$(body.ISSUE_REPO)"
    _REPO_BRANCH   = "$(body.REPO_BRANCH)"
    _PRODUCT_AREAS = "$(body.PRODUCT_AREAS)"
    _FORCE_RERUN   = "$(body.FORCE_RERUN)"
  }

  build {
    timeout       = local.pg_diagnosis_build.timeout
    substitutions = local.pg_diagnosis_build.substitutions

    options {
      substitution_option = local.pg_diagnosis_build.options.substitutionOption
      logging             = local.pg_diagnosis_build.options.logging
    }

    available_secrets {
      dynamic "secret_manager" {
        for_each = local.pg_diagnosis_build.availableSecrets.secretManager
        content {
          env          = secret_manager.value.env
          version_name = secret_manager.value.versionName
        }
      }
    }

    dynamic "step" {
      for_each = local.pg_diagnosis_build.steps
      content {
        name       = step.value.name
        id         = lookup(step.value, "id", null)
        args       = lookup(step.value, "args", null)
        entrypoint = lookup(step.value, "entrypoint", null)
        env        = lookup(step.value, "env", null)
        secret_env = lookup(step.value, "secretEnv", null)
        dir        = lookup(step.value, "dir", null)
      }
    }
  }
}

# ── Imports ──────────────────────────────────────────────────────────────────
# Every resource below already exists, created by setup_gcp.sh before this
# Terraform knew about it (TODO(#77085)). Import so apply doesn't try (and
# fail) to create them fresh. The staging-only resources gate their import on
# the same condition as the resource. Remove these blocks after the first
# successful apply in both environments.

# The Docs API is enabled and the SA exists in both staging and prod (the prod
# SA is bare — no roles, secrets, or group memberships yet; see TODO(#70351)).
# Gated so plans against dev/sandbox projects don't try to import resources
# that don't exist there.
import {
  for_each = contains(["recidiviz-staging", "recidiviz-123"], var.project_id) ? toset([var.project_id]) : toset([])
  id       = "${each.value}/docs.googleapis.com"
  to       = google_project_service.docs_api
}

import {
  for_each = contains(["recidiviz-staging", "recidiviz-123"], var.project_id) ? toset([var.project_id]) : toset([])
  id       = "projects/${each.value}/serviceAccounts/diagnosis-for-pg-ticket@${each.value}.iam.gserviceaccount.com"
  to       = google_service_account.pg_diagnosis
}

# roles/artifactregistry.reader is absent because it is a new grant (the live
# binding is the broader roles/artifactregistry.writer, which the docker-build
# step needed before #98473; it is left unmanaged and should be revoked by
# hand once reader is applied).
import {
  for_each = local.pg_diagnosis_enabled ? toset([
    "roles/bigquery.dataViewer",
    "roles/bigquery.jobUser",
    "roles/cloudbuild.builds.editor",
    "roles/logging.logWriter",
  ]) : toset([])
  id = "${var.project_id} ${each.value} serviceAccount:diagnosis-for-pg-ticket@${var.project_id}.iam.gserviceaccount.com"
  to = google_project_iam_member.pg_diagnosis_runtime_iam[each.value]
}

import {
  for_each = local.pg_diagnosis_enabled ? toset([var.project_id]) : toset([])
  id       = "projects/${each.value}/serviceAccounts/diagnosis-for-pg-ticket@${each.value}.iam.gserviceaccount.com roles/iam.serviceAccountTokenCreator serviceAccount:diagnosis-for-pg-ticket@${each.value}.iam.gserviceaccount.com"
  to       = google_service_account_iam_member.pg_diagnosis_self_impersonation[0]
}

import {
  for_each = local.pg_diagnosis_enabled ? toset([var.project_id]) : toset([])
  id       = "projects/${each.value}/secrets/pg_diagnosis_claude_api_key"
  to       = google_secret_manager_secret.pg_diagnosis_claude_api_key[0]
}

import {
  for_each = local.pg_diagnosis_enabled ? toset([var.project_id]) : toset([])
  id       = "projects/${each.value}/secrets/github_pg_diagnosis_webhook"
  to       = google_secret_manager_secret.github_pg_diagnosis_webhook[0]
}

import {
  for_each = local.pg_diagnosis_enabled ? toset([
    "pg_diagnosis_claude_api_key",
    "github_deploy_script_pat",
    "linear_deploy_script_api_key",
  ]) : toset([])
  id = "projects/${var.project_id}/secrets/${each.value} roles/secretmanager.secretAccessor serviceAccount:diagnosis-for-pg-ticket@${var.project_id}.iam.gserviceaccount.com"
  to = google_secret_manager_secret_iam_member.pg_diagnosis_secret_access[each.value]
}

import {
  for_each = local.pg_diagnosis_enabled ? toset([var.project_id]) : toset([])
  id       = "projects/${each.value}/secrets/github_pg_diagnosis_webhook roles/secretmanager.secretAccessor serviceAccount:service-${data.google_project.current.number}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
  to       = google_secret_manager_secret_iam_member.pg_diagnosis_webhook_cloudbuild_agent[0]
}

# Membership ids come from `gcloud identity groups memberships list
# --group-email=<group>`; the id is the member's identity, so it is the same
# in every group.
import {
  for_each = local.pg_diagnosis_enabled ? local.pg_diagnosis_data_access_groups : {}
  id       = "${each.value}/memberships/111134772127459829990"
  to       = google_cloud_identity_group_membership.pg_diagnosis_state_data_group[each.key]
}
