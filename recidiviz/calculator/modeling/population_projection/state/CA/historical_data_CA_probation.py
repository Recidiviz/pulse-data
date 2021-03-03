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

STATE: CA
POLICY: probation cap reduction
VERSION: v2
DATA SOURCE: https://docs.google.com/spreadsheets/d/1s9bGr7KC8HRlICHszitBo8TJICnqVZZkSs3VQ2dNs3I/edit?ts=5f583d5c#gid=1147338138
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: LOS distribution for probation by felony vs misdemeanor
ADDITIONAL NOTES: model built to support policy memo for CA AB1950
"""

import pandas as pd
from numpy import mean
from scipy.stats import chi2
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import (
    upload_spark_model_inputs,
)

# pylint: skip-file


# RAW DATA
crimes = ["felony", "misdemeanor"]
reference_year = 2016

total_population = pd.DataFrame(
    {
        "time_step": [i - reference_year for i in range(2011, 2020)],
        "felony": [
            247770,
            249173,
            254106,
            244122,
            221243,
            190686,
            183623,
            166745,
            161120,
        ],
        "misdemeanor": [50147, 45820, 42858, 41559, 42288, 49049, 49423, 43018, 38193],
    }
)

discharges = pd.DataFrame(
    {
        "time_step": [i - reference_year for i in range(2011, 2020)],
        "felony": [
            134055,
            128129,
            134849,
            134970,
            119320,
            103172,
            100745,
            102212,
            93245,
        ],
        "misdemeanor": [32984, 33414, 29911, 28105, 41846, 32994, 33198, 34426, 29294],
    }
)

admissions = pd.DataFrame(
    {
        "time_step": [i - reference_year for i in range(2011, 2020)],
        "felony": [
            131626,
            132387,
            142904,
            140890,
            111689,
            104045,
            104146,
            90836,
            89724,
        ],
        "misdemeanor": [29061, 28311, 28611, 41361, 41831, 34831, 33266, 28821, 25078],
    }
)

revocations = pd.DataFrame(
    {
        "time_step": [i - reference_year for i in range(2011, 2020)],
        "felony": [48628, 47670, 54126, 53060, 46226, 39804, 38759, 35757, 37763],
        "misdemeanor": [12497, 11979, 10771, 9882, 14125, 13584, 13716, 10727, 10129],
    }
)

# TRANSITIONS TABLE
mean_revocation_fraction_of_discharges = {
    crime: mean(revocations[crime]) / mean(discharges[crime]) for crime in crimes
}

mean_completion_duration = {
    crime: (1 - mean_revocation_fraction_of_discharges[crime])
    * mean(total_population[crime])
    / (mean(discharges[crime]) - mean(revocations[crime]))
    for crime in crimes
}

transitions_data = pd.DataFrame()
for crime in crimes:
    # populate transition data
    completion_pdf = chi2(mean_completion_duration[crime]).pdf
    probation_transition_table = pd.DataFrame(
        {
            "compartment": ["probation"] * 100,
            "compartment_duration": [i + 1 for i in range(50)] * 2,
            "outflow_to": ["release"] * 50 + ["prison"] * 50,
            "total_population": [completion_pdf(i + 1) for i in range(50)]
            + [
                completion_pdf(i + 1) * mean_revocation_fraction_of_discharges[crime]
                for i in range(50)
            ],
            "crime_type": [crime] * 100,
        }
    )
    secondary_transition_table = pd.DataFrame(
        {
            "compartment": ["release", "prison"],
            "compartment_duration": [1, 1],
            "outflow_to": ["release", "prison"],
            "total_population": [1, 1],
            "crime_type": [crime, crime],
        }
    )
    transitions_data = pd.concat(
        [transitions_data, probation_transition_table, secondary_transition_table]
    )

# OUTFLOWS TABLE
outflows_data = pd.DataFrame()
for crime in crimes:
    admissions_outflows_data = pd.DataFrame(
        {
            "compartment": ["prison_shell"] * 9,
            "time_step": admissions["time_step"],
            "outflow_to": ["probation"] * 9,
            "total_population": admissions[crime],
            "crime_type": [crime] * 9,
        }
    )
    discharges_outflows_data = pd.DataFrame(
        {
            "compartment": ["probation"] * 9,
            "time_step": discharges["time_step"],
            "outflow_to": ["release"] * 9,
            "total_population": discharges[crime],
            "crime_type": [crime] * 9,
        }
    )
    revocations_outflows_data = pd.DataFrame(
        {
            "compartment": ["probation"] * 9,
            "time_step": revocations["time_step"],
            "outflow_to": ["prison"] * 9,
            "total_population": revocations[crime],
            "crime_type": [crime] * 9,
        }
    )
    outflows_data = pd.concat(
        [
            outflows_data,
            admissions_outflows_data,
            discharges_outflows_data,
            revocations_outflows_data,
        ]
    )

# TOTAL POPULATION DATA
total_population_data = pd.DataFrame()
for crime in ["felony", "misdemeanor"]:
    crime_population_data = total_population[["time_step", crime]]
    crime_population_data = crime_population_data.rename(
        {crime: "total_population"}, axis=1
    )
    crime_population_data["crime_type"] = crime
    total_population_data = pd.concat([total_population_data, crime_population_data])
total_population_data["compartment"] = "probation"
total_population_data["time_step"] = total_population_data["time_step"].apply(int)

# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging",
    "CA_probation",
    outflows_data,
    transitions_data,
    total_population_data,
)
