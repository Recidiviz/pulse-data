# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
Historical data to be ingested for a particular state x policy combination
file name should be `historical_data_{state_code}_{primary_compartment}.py`
    where state_code is of form NJ and primary compartment is the tag of the main compartment relevant to the policy
DATA PRE-PROCESSING TASK: match naming convention for file name

DATA PRE-PROCESSING TASK: fill this out
STATE: OH
POLICY: Reclassifying drug posession as misdemeanor
VERSION: V1
DATA SOURCE: https://docs.google.com/document/d/1_D2dq5CehLC-b0jT85jyxBPSprNL96BImXmX7kVXQ9s/edit?usp=sharing
DATA QUALITY: reasonable/good
HIGHEST PRIORITY MISSING DATA: population data based on offense - currently estimating drug possession prison population based on % commitments that are for drug possession. Also missing detailed data to estimate the impact of the policy (i.e. % of target population eligible for treatment under new policy)
REFERENCE_DATE: 2020
TIME_STEP: year
ADDITIONAL NOTES:
"""

# DATA PRE-PROCESSING TASK: add whatever modules you need
import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

# pylint: skip-file


# RAW DATA
# DATA PRE-PROCESSING TASK: write in all raw data below


# DATA PRE-PROCESSING TASK: add one column to transitions_data & outflows_data per disaggregation axis.
#  If none exist, add place-holder axis
transitions_data = pd.read_csv("oh_p1_transitions.csv")
outflows_data = pd.read_csv("oh_p1_outflows.csv")
total_population_data = pd.read_csv("oh_p1_population.csv")

# TRANSITIONS TABLE
# DATA PRE-PROCESSING TASK: populate transitions_data from raw data

# OUTFLOWS TABLE
# DATA PRE-PROCESSING TASK: populate outflows_data from raw data

# TOTAL POPULATION TABLE
# DATA PRE-PROCESSING TASK: populate total_population_data from raw data

# STORE DATA
# DATA PRE-PROCESSING TASK: fill in `state` and `primary_compartment`
simulation_tag = "OH_SB3_prong1"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/OH/Prong1/OH_prison_p1_model_inputs.yaml",
)
