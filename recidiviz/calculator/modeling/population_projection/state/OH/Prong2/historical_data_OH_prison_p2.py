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
"""
Historical data to be ingested for a particular state x policy combination
file name should be `historical_data_{state_code}_{primary_compartment}.py`
    where state_code is of form NJ and primary compartment is the tag of the main compartment relevant to the policy
TODO(#99999): match naming convention for file name

TODO(#99999): fill this out
STATE: OH
POLICY: changing drug trafficking charge
VERSION: V1
DATA SOURCE: https://docs.google.com/document/d/1_D2dq5CehLC-b0jT85jyxBPSprNL96BImXmX7kVXQ9s/edit?usp=sharing
DATA QUALITY: reasonable/good
HIGHEST PRIORITY MISSING DATA: [fill this out]
REFERENCE_DATE: 2020
TIME_STEP: year
ADDITIONAL NOTES:
"""

# TODO(#99999): add whatever modules you need
import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

# pylint: skip-file


# RAW DATA
# TODO(#99999): write in all raw data below


# TODO(#99999): add one column to transitions_data & outflows_data per disaggregation axis.
#  If none exist, add place-holder axis
transitions_data = pd.read_csv("oh_p2_transitions.csv")
outflows_data = pd.read_csv("oh_p2_outflows.csv")
total_population_data = pd.read_csv("oh_p2_population.csv")


transitions_data = transitions_data.rename(
    {"charge": "crime_type", "felony_level": "crime"}, axis=1
)
outflows_data = outflows_data.rename(
    {"charge": "crime_type", "felony_level": "crime"}, axis=1
)
total_population_data = total_population_data.rename(
    {"charge": "crime_type", "felony_level": "crime"}, axis=1
)

# TRANSITIONS TABLE

transitions_data.compartment_duration *= 12

# OUTFLOWS TABLE

final_outflows = pd.DataFrame()
for year in outflows_data.time_step.unique():
    year_outflows = outflows_data[outflows_data.time_step == year]
    for month in range(12):
        month_outflows = year_outflows.copy()
        month_outflows.time_step = 12 * month_outflows.time_step - month
        month_outflows.total_population /= 12
        final_outflows = pd.concat([final_outflows, month_outflows])
outflows_data = final_outflows

# TOTAL POPULATION TABLE

final_pops = pd.DataFrame()
for year in total_population_data.time_step.unique():
    year_pops = total_population_data[total_population_data.time_step == year]
    for month in range(12):
        month_pops = year_pops.copy()
        month_pops.time_step = 12 * month_pops.time_step - month
        final_pops = pd.concat([final_pops, month_pops])
total_population_data = final_pops

# STORE DATA
# TODO(#99999): fill in `state` and `primary_compartment`
simulation_tag = "OH_SB3_prong2"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/OH/Prong2/OH_prison_p2_model_inputs.yaml",
)
