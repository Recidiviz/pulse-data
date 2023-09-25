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

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.utils.yaml_dict import YAMLDict

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

AZ_DIRECTORY_PATH = "recidiviz/calculator/modeling/population_projection/state/AZ/drug_possession_defelonization/"

# Get the simulation tag from the model inputs config
yaml_file_path = AZ_DIRECTORY_PATH + "AZ_drug_possession_defelonization.yaml"
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

# Input files are available at
# https://docs.google.com/spreadsheets/d/1MNvNklMWBjpTfRD05Y2rGPQ33eoM-kDRpzFHzqLf1Tk/edit#gid=0
outflows = pd.read_csv(
    AZ_DIRECTORY_PATH + "AZ Possession Defelonization Data - Copy of outflows.csv"
)
outflows["total_population"] = outflows["total_population"].astype(float)

baseline_transitions = pd.read_csv(
    AZ_DIRECTORY_PATH + "AZ Possession Defelonization Data - Copy of transitions.csv"
)
baseline_transitions["compartment_duration"] = baseline_transitions[
    "compartment_duration"
].astype(float)
baseline_transitions["total_population"] = baseline_transitions[
    "total_population"
].astype(float)

total_population = pd.read_csv(
    "AZ Possession Defelonization Data - Copy of population.csv"
)
total_population["total_population"] = total_population["total_population"].astype(
    float
)

# Store the input tables in BigQuery
upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    admissions_data_df=outflows,
    transitions_data_df=baseline_transitions,
    population_data_df=total_population,
    yaml_path=yaml_file_path,
)
