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
STATE: Massachusetts
POLICY: SB 920 Raise the age
VERSION: V1
DATA SOURCE: https://www.dropbox.com/sh/5lze162cztl4tfd/AACrroh0aY9elWxKsSZ4Urkha?dl=0
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA:
REFERENCE_DATE: November 2020
TIME_STEP: Month
ADDITIONAL NOTES: Initial policy scoping doc https://docs.google.com/document/d/1mj6Fmm3aCmx08PqhNShV6Rb8MCRuHQ2D56BmxeJKxKg/edit?usp=sharing
"""

import logging

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.utils.yaml_dict import YAMLDict

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

MA_DIRECTORY_PATH = (
    "recidiviz/calculator/modeling/population_projection/state/MA/raise_the_age/"
)

# Get the simulation tag from the model inputs config
yaml_file_path = MA_DIRECTORY_PATH + "MA_raise_the_age_inputs.yaml"
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

# Input data available at
# https://docs.google.com/spreadsheets/d/14yzlaU_--GLZOl8Qxu4c0x0VLiYWi7UDzMWUm54wT4Y/edit?usp=sharing
outflows = pd.read_csv("New MA data - Copy of estimated outflows.csv")
outflows["total_population"] = outflows["total_population"].astype(float)
outflows["age"] = outflows["age"].astype(str)

baseline_transitions = pd.read_csv("New MA data - baseline transitions.csv")
baseline_transitions["compartment_duration"] = baseline_transitions[
    "compartment_duration"
].astype(float)
baseline_transitions["total_population"] = baseline_transitions[
    "total_population"
].astype(float)
baseline_transitions["age"] = baseline_transitions["age"].astype(str)

total_population = pd.read_csv("MA RTA Data - Copy of population.csv")
total_population["total_population"] = total_population["total_population"].astype(
    float
)
total_population["age"] = total_population["age"].astype(str)

# Store the input tables in BigQuery
upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    outflows_data_df=outflows,
    transitions_data_df=baseline_transitions,
    total_population_data_df=total_population,
    yaml_path=yaml_file_path,
)
