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

# Build steps are generated from Python code. To update, run:
#   python -m recidiviz.tools.deploy.cloud_build.generate_terraform_plan_pr_commenter_build_steps
locals {
  pr_commenter_build_config = yamldecode(file("config/terraform_plan_pr_commenter_build_steps.yaml"))
}

data "google_project" "current" {}

# Trigger fired by GitHub Actions webhook (.github/workflows/terraform-plan-commenter.yml)
# whenever a PR modifies Terraform-related files. Runs `terraform plan` and
# posts the output as a comment on the PR.
resource "google_cloudbuild_trigger" "terraform_plan_pr_commenter" {
  provider = google-beta
  count    = var.project_id == "recidiviz-staging" ? 1 : 0
  name     = "terraform-plan-pr-commenter"
  # Triggers that reference a 2nd-gen repository resource (see source_to_build
  # below) must live in the same region as the repository connection; they
  # cannot be created in the default "global" region.
  location    = "us-west1"
  description = "Generates and comments Terraform plans on Github Pull Requests"
  # This service account is hand-created in the GCP console; it is not
  # declared in Terraform. It must also hold a Google Workspace admin role
  # granting "Groups > Read" (assigned at admin.google.com → Admin roles), so
  # that `terraform plan`'s refresh of `google_cloud_identity_group_membership`
  # resources can read Workspace-backed groups — that authorization lives in
  # Workspace, not GCP IAM.
  service_account = "projects/${var.project_id}/serviceAccounts/terraform-plan-pr-commenter@${var.project_id}.iam.gserviceaccount.com"

  webhook_config {
    secret = "projects/${data.google_project.current.number}/secrets/github_terraform_commenter_webhook/versions/1"
  }

  source_to_build {
    repository = "projects/${var.project_id}/locations/us-west1/connections/Github/repositories/Recidiviz-pulse-data"
    ref        = "refs/heads/main"
    repo_type  = "GITHUB"
  }

  substitutions = {
    _COMMIT_REF  = "$(body.COMMIT_REF)"
    _PR_NUMBER   = "$(body.PR_NUMBER)"
    _PROJECT_ID  = var.project_id
    _VERSION_TAG = "latest"
  }

  build {
    logs_bucket   = local.pr_commenter_build_config.logs_bucket
    timeout       = local.pr_commenter_build_config.timeout
    substitutions = local.pr_commenter_build_config.substitutions
    tags          = local.pr_commenter_build_config.tags

    options {
      machine_type        = local.pr_commenter_build_config.options.machine_type
      substitution_option = local.pr_commenter_build_config.options.substitution_option
    }

    available_secrets {
      dynamic "secret_manager" {
        for_each = local.pr_commenter_build_config.available_secrets.secret_manager
        content {
          env          = secret_manager.value.env
          version_name = secret_manager.value.version_name
        }
      }
    }

    dynamic "step" {
      for_each = local.pr_commenter_build_config.steps
      content {
        name          = step.value.name
        id            = lookup(step.value, "id", null)
        args          = lookup(step.value, "args", null)
        entrypoint    = lookup(step.value, "entrypoint", null)
        env           = lookup(step.value, "env", null)
        secret_env    = lookup(step.value, "secret_env", null)
        wait_for      = lookup(step.value, "wait_for", null)
        allow_failure = lookup(step.value, "allow_failure", null)
        dir           = lookup(step.value, "dir", null)
        timeout       = lookup(step.value, "timeout", null)

        dynamic "volumes" {
          for_each = lookup(step.value, "volumes", [])
          content {
            name = volumes.value.name
            path = volumes.value.path
          }
        }
      }
    }
  }
}
