# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
resource "google_cloudbuild_trigger" "staging_release_build_trigger" {
  provider    = google-beta
  description = "Builds a remote Docker image for staging on every push to main."
  count       = var.project_id == "recidiviz-staging" ? 1 : 0

  github {
    owner = "Recidiviz"
    name  = "pulse-data"
    push {
      branch = "^main$|^releases/v[0-9]+.[0-9]+-rc$"
    }
  }

  build {
    step {
      name       = "gcr.io/cloud-builders/docker"
      entrypoint = "chmod"
      args       = ["a+w", "/workspace"]
      id         = "Give non-root users access to /workspace/ volume"
    }

    step {
      name       = "alpine"
      entrypoint = "sh"
      args = [
        "-c",
        join(" && ", [
          format("wget -O docker-credential-gcr.tar.gz %s", "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.22/docker-credential-gcr_linux_amd64-2.1.22.tar.gz"),
          "tar xz -f docker-credential-gcr.tar.gz docker-credential-gcr",
          "chmod +x docker-credential-gcr",
          "mkdir /workspace/gcloud",
          "mv docker-credential-gcr /workspace/gcloud"
        ])
      ]
      id = "download-docker-credential"
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      args = [
        "buildx",
        "create",
        "--name",
        "appengine"
      ]
      id       = "create-build-context-appengine"
      wait_for = ["download-docker-credential"]
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      env  = ["DOCKER_BUILDKIT=1"]
      args = [
        "-c",
        join(" && ", [
          "export PATH=\"/workspace/gcloud:$$${PATH}\"",
          "/workspace/gcloud/docker-credential-gcr configure-docker --registries=us-docker.pkg.dev",
          join(" ", [
            "docker buildx build . -f Dockerfile --builder appengine",
            "--tag=us-docker.pkg.dev/$PROJECT_ID/appengine/build:$COMMIT_SHA",
            "--cache-to",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/appengine/build:cache,mode=max",
            "--cache-from",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/appengine/build:cache",
            "--push"
          ])
        ])
      ]
      id         = "build-appengine"
      wait_for   = ["create-build-context-appengine"]
      entrypoint = "sh"
    }


    step {
      name = "gcr.io/cloud-builders/docker"
      args = [
        "buildx",
        "create",
        "--name",
        "recidiviz-base"
      ]
      id       = "create-build-context-recidiviz-base"
      wait_for = ["download-docker-credential"]
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      env  = ["DOCKER_BUILDKIT=1"]
      args = [
        "-c",
        join(" && ", [
          "export PATH=\"/workspace/gcloud:$$${PATH}\"",
          "/workspace/gcloud/docker-credential-gcr configure-docker --registries=us-docker.pkg.dev",
          join(" ", [
            "docker buildx build . -f Dockerfile.recidiviz-base --builder recidiviz-base",
            "--tag=us-docker.pkg.dev/$PROJECT_ID/recidiviz-base/default:latest",
            "--cache-to",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/recidiviz-base/build:cache,mode=max",
            "--cache-from",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/recidiviz-base/build:cache",
            "--push"
          ])
        ])
      ]
      id         = "build-recidiviz-base"
      wait_for   = ["create-build-context-recidiviz-base"]
      entrypoint = "sh"
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      args = [
        "buildx",
        "create",
        "--name",
        "asset-generation",
      ]
      id       = "create-build-context-asset-generation"
      wait_for = ["download-docker-credential"]
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      env  = ["DOCKER_BUILDKIT=1"]
      args = [
        "-c",
        join(" && ", [
          "export PATH=\"/workspace/gcloud:$$${PATH}\"",
          "/workspace/gcloud/docker-credential-gcr configure-docker --registries=us-docker.pkg.dev",
          join(" ", [
            "docker buildx build . -f Dockerfile.asset-generation --builder asset-generation",
            "--tag=us-docker.pkg.dev/$PROJECT_ID/asset-generation/build:$COMMIT_SHA",
            "--cache-to",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/asset-generation/build:cache,mode=max",
            "--cache-from",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/asset-generation/build:cache",
            "--push",
          ])
        ])
      ]
      id         = "build-asset-generation"
      wait_for   = ["create-build-context-asset-generation"]
      entrypoint = "sh"
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      args = [
        "buildx",
        "create",
        "--name",
        "case-triage-pathways",
      ]
      id       = "create-build-context-case-triage-pathways"
      wait_for = ["download-docker-credential"]
    }

    step {
      name = "gcr.io/cloud-builders/docker"
      env  = ["DOCKER_BUILDKIT=1"]
      args = [
        "-c",
        join(" && ", [
          "export PATH=\"/workspace/gcloud:$$${PATH}\"",
          "/workspace/gcloud/docker-credential-gcr configure-docker --registries=us-docker.pkg.dev",
          join(" ", [
            "docker buildx build . -f Dockerfile.case-triage-pathways --builder case-triage-pathways",
            "--tag=us-docker.pkg.dev/$PROJECT_ID/case-triage-pathways/default:latest",
            "--cache-to",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/case-triage-pathways/build:cache,mode=max",
            "--cache-from",
            "type=registry,ref=us-docker.pkg.dev/$PROJECT_ID/case-triage-pathways/build:cache",
            "--push",
          ])
        ])
      ]
      id         = "build-case-triage-pathways"
      wait_for   = ["create-build-context-case-triage-pathways"]
      entrypoint = "sh"
    }
    options {
      machine_type = "N1_HIGHCPU_32"
    }
    timeout = "3600s"
  }

  depends_on = [
    google_artifact_registry_repository.repositories
  ]
}
