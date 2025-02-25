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
# pylint: skip-file

"""
STATE: Arizona
POLICY: Drug possession defelonization
VERSION: V1
DATA SOURCE:
DATA QUALITY:
HIGHEST PRIORITY MISSING DATA:
REFERENCE_DATE: December 2019
TIME_STEP: Month
ADDITIONAL NOTES:
"""

import logging
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.relpath("../../../../../../.."))

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.utils.yaml_dict import YAMLDict

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

AZ_DIRECTORY_PATH = ""

# Get the simulation tag from the model inputs config
yaml_file_path = AZ_DIRECTORY_PATH + "AZ_drug_possession_defelonization.yaml"
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

# Input files are available at
# https://docs.google.com/spreadsheets/d/1MNvNklMWBjpTfRD05Y2rGPQ33eoM-kDRpzFHzqLf1Tk/edit#gid=0
admissions = pd.read_csv(
    AZ_DIRECTORY_PATH + "AZ Possession Defelonization Data - Copy of outflows.csv"
)
admissions.rename(
    {
        "total_population": "cohort_population",
        "outflow_to": "admission_to",
        "crime_type": "simulation_group",
    },
    axis=1,
    inplace=True,
)
admissions["cohort_population"] = admissions["cohort_population"].astype(float)

baseline_transitions = pd.read_csv(
    AZ_DIRECTORY_PATH + "AZ Possession Defelonization Data - Copy of transitions.csv"
)
baseline_transitions.rename(
    {"total_population": "cohort_portion", "crime_type": "simulation_group"},
    axis=1,
    inplace=True,
)
baseline_transitions["compartment_duration"] = baseline_transitions[
    "compartment_duration"
].astype(float)
baseline_transitions["cohort_portion"] = baseline_transitions["cohort_portion"].astype(
    float
)

population = pd.read_csv("AZ Possession Defelonization Data - Copy of population.csv")
population.rename(
    {"total_population": "compartment_population", "crime_type": "simulation_group"},
    axis=1,
    inplace=True,
)
population["compartment_population"] = population["compartment_population"].astype(
    float
)

# Store the input tables in BigQuery
upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    admissions_data_df=admissions,
    transitions_data_df=baseline_transitions,
    population_data_df=population,
    yaml_path=yaml_file_path,
)
