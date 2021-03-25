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
STATE: CA
POLICY: Rebound Act
VERSION: ?
DATA SOURCE: Justice Counts Data
DATA QUALITY: reasonableish
HIGHEST PRIORITY MISSING DATA: LOS parole; accurate revocation numbers and types
ADDITIONAL NOTES:

"""

import pandas as pd
import sys

# print(sys.path)
# sys.path.insert(0,'../..')
# sys.path.insert(0, os.path.relpath('../../../../..'))
# print(sys.path)
# from spark_bq_utils import upload_spark_model_inputs
# from spark_bq_utils import upload_spark_model_inputs
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

pd.set_option("display.max_rows", None)
# pylint: skip-file

# RAW DATA
# time step length = 1 month
# reference_year = 2014
# DISAGGREGATION AXES
# none, using 'crime'

# OUTFLOWS TABLE (presupervision to parole)
# annual data from Offender Data Points reports https://www.cdcr.ca.gov/research/offender-outcomes-characteristics/offender-data-points/
# 2019 data based on first six months only (I take the July - June data, subtract half the prior year's number (to account for July-Dec, and
# then add half the July - June number (to account for Jan - June.)))
outflows_data_ps_to_parole = pd.DataFrame(
    {
        "time_step": [0, 1, 2, 3, 4, 5],
        "compartment": ["pre-supervision"] * 6,
        "outflow_to": ["parole"] * 6,
        "crime": [1, 1, 1, 1, 1, 1],
        "total_population": [19658, 19048, 18200, 17003, 20528, 19700],
    }
)

outflows_data_parole_to_ps = pd.DataFrame(
    {
        "time_step": [0, 1, 2, 3, 4, 5],
        "compartment": ["parole"] * 6,
        "outflow_to": ["prison"] * 6,
        "crime": [1, 1, 1, 1, 1, 1],
        "total_population": [4522, 4357, 4679, 4188, 4027, 5269],
    }
)
outflows_data = pd.concat([outflows_data_parole_to_ps, outflows_data_ps_to_parole])

final_outflows = pd.DataFrame()
for year in outflows_data.time_step.unique():
    year_outflows = outflows_data[outflows_data.time_step == year]
    for month in range(12):
        month_outflows = year_outflows.copy()
        month_outflows.time_step = 12 * month_outflows.time_step - month
        month_outflows.total_population /= 12
        final_outflows = pd.concat([final_outflows, month_outflows])
outflows_data = final_outflows

# TRANSITIONS TABLE (parole to prison)
one_yr_rate = 0.179
two_yr_rate = 0.336
three_yr_rate = 0.433
released = 1 - three_yr_rate
transitions_data = pd.DataFrame(
    {
        "compartment": ["parole", "parole", "parole", "parole", "prison", "release"],
        "outflow_to": ["prison", "prison", "prison", "release", "prison", "release"],
        "crime": [1, 1, 1, 1, 1, 1],
        "compartment_duration": [12, 24, 36, 36, 36, 36],
        "total_population": [
            one_yr_rate,
            two_yr_rate - one_yr_rate,
            three_yr_rate - two_yr_rate,
            released,
            1,
            1,
        ],
    }
)

# TOTAL POPULATION TABLE (parole to new offense revocation)
total_population_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/CA/parole_total_population.csv"
)

# STORE DATA
# state = 'CA'
# primary_compartment = 'parole'
# pd.concat([transitions_data, outflows_data, total_population_data], sort=False).to_csv(
#     f'recidiviz/calculator/modeling/population_projection/state/{state}/preprocessed_data_{state}_{primary_compartment}.csv')
print("OUTFLOWS = ", outflows_data)
print("TRANSITIONS = ", transitions_data)
print("TOTAL POP = ", total_population_data)


upload_spark_model_inputs(
    "recidiviz-staging",
    "CA_parole",
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/CA/PO_incentives/CA_parole_model_inputs.yaml",
)
