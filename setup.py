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

# Packages required by the pipeline
REQUIRED_PACKAGES = [
    'apache-beam',
    'cattrs',
    'dateparser',
    'Flask',
    'google-api-python-client',
    'google-cloud-monitoring',
    'more-itertools',
    'oauth2client',
    'opencensus @ git+https://github.com/census-instrumentation/'
        'opencensus-python.git@d37b6d267307a136631881e593c2bc8921a786b6#egg'
        '=opencensus',
    'opencensus-correlation @ git+https://github.com/census-instrumentation/'
        'opencensus-python.git@d37b6d267307a136631881e593c2bc8921a786b6#egg='
        'opencensus_correlation&subdirectory=contrib/opencensus-correlation',
    'SQLAlchemy'
]

setuptools.setup(
    name='recidiviz-calculation-pipelines',
    # TODO(2031): Dynamically set the package version
    version='1.0.43',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
)
