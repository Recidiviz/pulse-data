# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
    "aenum",
    "apache-beam",
    "cattrs",
    "dateparser",
    # Must stay up-to-date with latest dill version in the Pipfile - this library is used for template serialization and
    # it's critical that the local version used to serialize the templates and the version used to deserialize on the
    # remote workers match.
    "dill==0.3.3",
    "Flask",
    "google-api-core",
    "google-api-python-client",
    # TODO(#4231): Pinned due to dependency version introduced in version 2.0.0 on 10/5/20
    "google-cloud-monitoring==1.1.0",
    "google-cloud-secret-manager",
    "html5lib",
    "lxml",
    "more-itertools",
    "oauth2client",
    "opencensus",
    "opencensus-correlation",
    "opencensus-ext-stackdriver",
    # Must stay up-to-date with latest protobuf version in the Pipfile
    "protobuf==3.17.1",
    "SQLAlchemy",
    "us",
]

setuptools.setup(
    name="recidiviz-calculation-pipelines",
    # TODO(#2031): Dynamically set the package version
    version="1.0.76",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    package_data={"recidiviz.common": ["data_sets/*.csv"]},
)
