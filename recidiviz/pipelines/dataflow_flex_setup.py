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
This is not used to set up the entire recidiviz package. The required packages are
read from dataflow_flex_requirements.txt, which always sits alongside this file: in
the repo, in the flex template image (see Dockerfile.pipelines), and in the sdist
Beam builds from this file and stages to Dataflow workers (see
dataflow_flex_MANIFEST.in).

This module runs standalone -- under `pip install` in the flex template image and
again when the staged sdist is installed on each worker -- so it must not import
anything outside the standard library and setuptools.
"""
import os

import setuptools

REQUIREMENTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "dataflow_flex_requirements.txt"
)


def _required_packages() -> list[str]:
    """Returns the pinned requirements from dataflow_flex_requirements.txt, ignoring
    blank lines and '#' comments."""
    with open(REQUIREMENTS_PATH, "r", encoding="utf-8") as requirements_file:
        lines = [line.strip() for line in requirements_file]

    return [line for line in lines if line and not line.startswith("#")]


setuptools.setup(
    name="pulse-dataflow-pipelines",
    # TODO(#2031): Dynamically set the package version
    version="1.0.0",
    install_requires=_required_packages(),
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
        "recidiviz.pipelines.ingest.identity.fragment_attribute_conflict_checking": [
            "*.csv"
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
