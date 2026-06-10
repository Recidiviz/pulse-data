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
#
# TODO(#70351): Switch to recidiviz-123 after getting prod SA permissions.
resource "google_cloudbuild_trigger" "pg_diagnosis" {
  provider = google-beta
  count    = var.project_id == "recidiviz-staging" ? 1 : 0
  name     = "pg-diagnosis"
  # Triggers that reference a 2nd-gen repository resource (see source_to_build
  # below) must live in the same region as the repository connection; they
  # cannot be created in the default "global" region.
  location    = "us-west1"
  description = "Diagnoses incoming Product Growth issues and comments on them"
  # SA is created/configured by
  # recidiviz/tools/claude_workflows/pg_ticket_diagnosis/setup_gcp.sh until
  # TODO(#77085) moves it into Terraform. It is set here (not in cloudbuild.yaml)
  # — specifying it in both the trigger and the build config is rejected.
  service_account = "projects/${var.project_id}/serviceAccounts/diagnosis-for-pg-ticket@${var.project_id}.iam.gserviceaccount.com"

  webhook_config {
    secret = "projects/${data.google_project.current.number}/secrets/github_pg_diagnosis_webhook/versions/1"
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
    images        = local.pg_diagnosis_build.images

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
