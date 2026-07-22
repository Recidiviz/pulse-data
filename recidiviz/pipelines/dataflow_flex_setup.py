# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""This file is used *only* for deploying calculation pipelines in Dataflow.

This is referenced when running Dataflow pipelines or creating pipeline templates.
This is not used to set up the entire recidiviz package. The REQUIRED_PACKAGES
are the external packages required by the pipelines in ./recidiviz/calculator,
and must be manually updated any time a dependency is added to the project that
pipeline code touches.
"""
import setuptools

# Packages required by the pipeline. Dataflow workers have a list of packages already installed. To see this list, and
# which version of each package is installed, visit
# https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies
REQUIRED_PACKAGES = [
    # Do not include `apache-beam` in dataflow_flex_setup.py
    # All dependencies below must be pinned to an exact version: Dataflow workers
    # install this list fresh (via the Beam SDK harness setup_file mechanism) into a
    # separate venv at worker boot time, so an unpinned dependency is re-resolved
    # against PyPI at that point and a new upstream release can break every pipeline
    # without a code change here.
    "aiohttp==3.14.1",
    "cattrs==26.1.0",
    "cryptography==49.0.0",
    "dateparser==1.2.0",
    # Must stay up-to-date with latest dill and cloudpickle versions in pyproject.toml
    # these libraries are used for template serialization and
    # it's critical that the local version used to serialize the templates and the version used to deserialize on the
    # remote workers match.
    "dill==0.3.1.1",
    "cloudpickle==2.2.1",
    "Flask==3.1.3",
    "google-api-core==2.32.0",
    "google-api-python-client==2.198.0",
    "google-cloud-monitoring==2.31.0",
    # TODO(#28197): Add this package back when google resolves
    #  https://github.com/GoogleCloudPlatform/cloud-profiler-python/issues/142
    # "google-cloud-profiler",
    "google-cloud-secret-manager==2.29.0",
    "google-cloud-storage==2.19.0",
    "google-cloud-logging==3.16.0",
    "google-cloud-bigquery-datatransfer==3.23.0",
    # Must stay up-to-date with latest google-cloud-tasks version in the pyproject.toml
    "google-cloud-tasks==2.23.0",
    "iteration-utilities==0.13.0",
    "jsonschema==4.26.0",
    "more-itertools==11.1.0",
    "networkx==3.6.1",
    "oauth2client==4.1.3",
    # TODO(open-telemetry/opentelemetry-python#3959): Remove TODO when opentelemetry when cost increase is solved.
    "opentelemetry-api==1.33.1",
    "opentelemetry-sdk==1.33.1",
    "opentelemetry-exporter-gcp-monitoring==1.12.0a0",
    "opentelemetry-exporter-gcp-trace==1.12.0",
    "opentelemetry-resourcedetector-gcp==1.12.0a0",
    "opentelemetry-instrumentation-flask==0.54b1",
    "opentelemetry-instrumentation-grpc==0.54b1",
    "opentelemetry-instrumentation-redis==0.54b1",
    "opentelemetry-instrumentation-requests==0.54b1",
    "opentelemetry-instrumentation-sqlalchemy==0.54b1",
    "opentelemetry-semantic-conventions==0.54b1",
    # Must stay up-to-date with latest protobuf version in the pyproject.toml
    "protobuf==5.29.6",
    # Needed for thefuzz to avoid "Using slow pure-python SequenceMatcher" warning
    "python-Levenshtein==0.27.3",
    "psycopg2-binary==2.9.12",
    "SQLAlchemy==1.4.54",
    "thefuzz==0.22.1",
    "us==3.2.0",
]

setuptools.setup(
    name="pulse-dataflow-pipelines",
    # TODO(#2031): Dynamically set the package version
    version="1.0.0",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    package_data={
        "recidiviz.common": ["data_sets/*.csv"],
        "recidiviz.calculator.query.state.views": ["**/*.yaml"],
        "recidiviz.calculator.query.state.views.workflows": ["**/*.yaml"],
        "recidiviz.ingest.direct.regions": [
            "us_*/ingest_mappings/*.yaml",
            "us_*/*.yaml",
            "us_*/raw_data/*.yaml",
        ],
        "recidiviz.ingest.direct.ingest_mappings.yaml_schema": [
            "schema.json",
            "1.0.0/*/*.json",
        ],
        "recidiviz.pipelines": [
            "supplemental/template_metadata.json",
            "metrics/template_metadata.json",
        ],
        "recidiviz.monitoring": ["monitoring_instruments.yaml"],
        "recidiviz.validation.views.metadata.config": ["*.yaml"],
        "recidiviz.big_query.config": ["*.yaml"],
    },
)
