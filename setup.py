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

This is referenced when running the run_calculation_pipelines.py script.
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
    'aenum',
    'apache-beam',
    'cattrs',
    'dateparser',
    'Flask',
    # TODO(#3337): Remove this line once the google-api-core release is stable
    'google-api-core==1.17.0',
    # TODO(#2973): Resolve dependency conflict to remove this version constraint
    'google-api-python-client<=1.7.11',
    'google-cloud-monitoring',
    'more-itertools',
    'oauth2client',
    'opencensus',
    'opencensus-correlation',
    'opencensus-ext-stackdriver',
    # Must stay up-to-date with latest version
    'protobuf==3.13.0',
    'SQLAlchemy'
]

setuptools.setup(
    name='recidiviz-calculation-pipelines',
    # TODO(#2031): Dynamically set the package version
    version='1.0.76',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
)
