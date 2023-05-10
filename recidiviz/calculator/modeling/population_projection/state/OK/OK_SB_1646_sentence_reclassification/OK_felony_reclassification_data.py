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
STATE: Oklahoma
POLICY: Felony offense reclassification
VERSION: V1
DATA SOURCE:
DATA QUALITY:
HIGHEST PRIORITY MISSING DATA:
REFERENCE_DATE: December 2021
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

OK_DIRECTORY_PATH = "recidiviz/calculator/modeling/population_projection/state/OK/OK_SB_1646_sentence_reclassification/"
OK_POLICY_VERSION = "v3"

file_names = {
    "outflows": "outflows_v3 - outflows_v3.csv",
    "baseline_transitions": "trans_baseline_v3 - trans_baseline_v3.csv",
    "total_population": "pop_original - Copy of pop_original.csv",
    "yaml": f"OK_SB_1646_model_inputs.yaml",
}

# Inputs available at
# https://drive.google.com/drive/folders/1ms3xbhbNDgEhMeQDcS8N_e7FyTDdnS0o
# Use different input files for different policy implementations/versions
# OK_POLICY_VERSION = "v2"
#
# file_names = {
#     "outflows": "outflows_original - outflows_original.csv",
#     "baseline_transitions": "trans_baseline_original - trans_baseline_original.csv",
#     "total_population": "pop_original - Copy of pop_original.csv",
#     "yaml": f"OK_SB_1646_model_inputs.yaml",
# }

# OK_POLICY_VERSION = "reclassified_statutes"
#
# file_names = {
#     "outflows": "outflows_reclass - outflows_reclass.csv",
#     "baseline_transitions": "trans_baseline_reclass - trans_baseline_original.csv",
#     "total_population": "pop_original - Copy of pop_original.csv",
#     "yaml": f"OK_SB_1646_model_inputs_{OK_POLICY_VERSION}.yaml",
# }

# OK_POLICY_VERSION = "added_enhancements"
#
# file_names = {
#     "outflows": "outflows_enhancements - outflows_enhancements.csv",
#     "baseline_transitions": "trans_baseline_enhancements - trans_baseline_enhancements.csv",
#     "total_population": "pop_original - Copy of pop_original.csv",
#     "yaml": f"OK_SB_1646_model_inputs_{OK_POLICY_VERSION}.yaml",
# }

# OK_POLICY_VERSION = "hybrid"
#
# file_names = {
#     "outflows": "outflows_hybrid - outflows_enhancements.csv",
#     "baseline_transitions": "trans_baseline_hybrid - trans_baseline_enhancements.csv",
#     "total_population": "pop_original - Copy of pop_original.csv",
#     "yaml": f"OK_SB_1646_model_inputs_{OK_POLICY_VERSION}.yaml",
# }

OK_POLICY_DIRECTORY = OK_DIRECTORY_PATH + OK_POLICY_VERSION + "/"

# Get the simulation tag from the model inputs config
yaml_file_path = OK_DIRECTORY_PATH + file_names["yaml"]
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

outflows = pd.read_csv(OK_POLICY_DIRECTORY + file_names["outflows"])
outflows["total_population"] = outflows["total_population"].astype(float)

baseline_transitions = pd.read_csv(
    OK_POLICY_DIRECTORY + file_names["baseline_transitions"]
)
baseline_transitions["compartment_duration"] = baseline_transitions[
    "compartment_duration"
].astype(float)
baseline_transitions["total_population"] = baseline_transitions[
    "total_population"
].astype(float)

total_population = pd.read_csv(OK_POLICY_DIRECTORY + file_names["total_population"])
total_population["total_population"] = total_population["total_population"].astype(
    float
)

# Store the input tables in BigQuery
upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    outflows_data_df=outflows,
    transitions_data_df=baseline_transitions,
    total_population_data_df=total_population,
    yaml_path=yaml_file_path,
)
