# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
Historical data to be ingested for a particular state x policy combination.

File name should be `historical_data_{state_code}_{policy name}.py`
 where state_code is of form NJ.

DATA PRE-PROCESSING TASK: match naming convention for file name

DATA PRE-PROCESSING TASK: fill this out
STATE: [state_code]
POLICY: [one line policy description]
DATA QUALITY: [pick one of MVP/reasonable/great]
HIGHEST PRIORITY MISSING DATA: [one line data description(s)]
"""

# DATA PRE-PROCESSING TASK: add whatever modules you need
import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

# RAW DATA
# DATA PRE-PROCESSING TASK: write in all raw data below


# DATA PRE-PROCESSING TASK: add one column to transitions_data & outflows_data per disaggregation
#  axis. If none exist, add place-holder axis.
transitions_data = pd.DataFrame(
    columns=["compartment", "outflow_to", "total_population", "compartment_duration"]
)
outflows_data = pd.DataFrame(
    columns=["compartment", "outflow_to", "total_population", "time_step"]
)
total_population_data = pd.DataFrame(
    columns=["compartment", "total_population", "time_step"]
)

# TRANSITIONS TABLE
# DATA PRE-PROCESSING TASK: populate transitions_data from raw data

# OUTFLOWS TABLE
# DATA PRE-PROCESSING TASK: populate outflows_data from raw data

# TOTAL POPULATION TABLE
# DATA PRE-PROCESSING TASK: populate total_population_data from raw data

# STORE DATA
# DATA PRE-PROCESSING TASK: fill in `simulation_tag` and `path_to_your_yaml`
simulation_tag = "TKTK"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "path_to_your_yaml",
)
