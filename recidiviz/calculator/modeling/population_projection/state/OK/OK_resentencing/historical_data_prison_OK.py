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

import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs

two_or_more_priors = {2010: 35.67, 2011: 37.19, 2012: 37.95, 2013: 36.43, 2014: 36.43, 2015: 36.43, 2016: 33.40,
                      2017: 35.67, 2018: 31.88, 2019: 31.12}
outflows = {2010: 1390.00, 2011: 1240.00, 2012: 1258.00, 2013: 1234.00, 2014: 1612.00, 2015: 1738.00, 2016: 1893.00,
            2017: 1898.00, 2018: 1721.00, 2019: 972}
releases = {2010: 1532.00, 2011: 1428.00, 2012: 1235.00, 2013: 1150.00, 2014: 1327.00, 2015: 1362.00, 2016: 1817.00,
            2017: 2018.00, 2018: 1927.00, 2019: 1707.00}
outflows_data = pd.DataFrame({
    'compartment': ['pretrial'] * 10 + ['prison'] * 10,
    'outflow_to': ['prison'] * 10 + ['release'] * 10,
    'time_step': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] * 2,
    'total_population': [two_or_more_priors[y] * outflows[y] / 100 for y in outflows] + [
        two_or_more_priors[y] * releases[y] / 100 for y in outflows],
    'crime_type': ['NA'] * 20
})

final_outflows = pd.DataFrame()
for year in outflows_data.time_step.unique():
    year_outflows = outflows_data[outflows_data.time_step == year]
    for month in range(12):
        month_outflows = year_outflows.copy()
        month_outflows.time_step = 12 * month_outflows.time_step + month
        month_outflows.total_population /= 12
        final_outflows = pd.concat([final_outflows, month_outflows])
outflows_data = final_outflows

transitions_data = pd.DataFrame({
    'compartment': ['prison'] * 2 + ['release'] * 36 + ['release', 'release_full'],
    'outflow_to': ['release'] * 2 + ['prison'] * 36 + ['release_full', 'release_full'],
    'compartment_duration': [2400.80 / 365 * 12, 2457.60 / 365 * 12] + list(range(1, 37)) + [36, 36],
    'total_population': [1.0] * 2 + [0.23 / 36] * 36 + [0.77, 1],
    'crime_type': ['NA'] * 40
})

upload_spark_model_inputs('recidiviz-staging', 'OK_resentencing', outflows_data, transitions_data, pd.DataFrame())
