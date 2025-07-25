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
    "cattrs",
    "dateparser",
    # Must stay up-to-date with latest dill and cloudpickle versions in the Pipfile
    # these libraries are used for template serialization and
    # it's critical that the local version used to serialize the templates and the version used to deserialize on the
    # remote workers match.
    "dill==0.3.1.1",
    "cloudpickle==2.2.1",
    "Flask",
    "google-api-core",
    "google-api-python-client",
    "google-cloud-monitoring",
    # TODO(#28197): Add this package back when google resolves
    #  https://github.com/GoogleCloudPlatform/cloud-profiler-python/issues/142
    # "google-cloud-profiler",
    "google-cloud-secret-manager",
    "google-cloud-storage",
    "google-cloud-logging",
    "google-cloud-bigquery-datatransfer",
    # Must stay up-to-date with latest google-cloud-tasks version in the Pipfile
    "google-cloud-tasks==2.19.3",
    "iteration-utilities",
    "jsonschema",
    "more-itertools",
    "networkx",
    "oauth2client",
    # TODO(open-telemetry/opentelemetry-python#3959): Remove TODO when opentelemetry when cost increase is solved.
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-gcp-monitoring",
    "opentelemetry-exporter-gcp-trace",
    "opentelemetry-resourcedetector-gcp",
    "opentelemetry-instrumentation-flask",
    "opentelemetry-instrumentation-grpc",
    "opentelemetry-instrumentation-redis",
    "opentelemetry-instrumentation-requests",
    "opentelemetry-instrumentation-sqlalchemy",
    "opentelemetry-semantic-conventions",
    # Must stay up-to-date with latest protobuf version in the Pipfile
    "protobuf==6.31.1",
    # Needed for thefuzz to avoid "Using slow pure-python SequenceMatcher" warning
    "python-Levenshtein",
    "psycopg2-binary",
    "pytablewriter",
    "SQLAlchemy==1.4.54",
    "thefuzz",
    "us",
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
        "recidiviz.tools": ["deploy/terraform/config/*.yaml"],
        "recidiviz.validation.views.metadata.config": ["*.yaml"],
    },
)
