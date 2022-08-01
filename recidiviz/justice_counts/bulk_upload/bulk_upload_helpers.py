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
"""Helpers for bulk upload functionality."""

from typing import List, Optional, Type

import attr
from thefuzz import fuzz

from recidiviz.common.text_analysis import TextAnalyzer
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.jails_and_prisons import (
    CorrectionalFacilityForceType,
    CorrectionalFacilityStaffType,
    PrisonPopulationType,
    PrisonReleaseTypes,
    ReadmissionType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseSeverityType,
    DispositionType,
    ProsecutionAndDefenseStaffType,
)
from recidiviz.justice_counts.metrics import prisons, prosecution
from recidiviz.justice_counts.metrics.metric_definition import MetricDefinition
from recidiviz.persistence.database.schema.justice_counts import schema

FUZZY_MATCHING_SCORE_CUTOFF = 90


@attr.define()
class MetricFile:
    """Describes the structure of a CSV file for a particular Justice Counts metric.
    If the metric has <= 1 disaggregation, there will be one corresponding file.
    If the metric has multiple disaggregations (e.g. gender and race) there will be
    one CSV for each disaggregation.
    """

    # Allowed names of the CSV file (minus the .csv extension).
    # We use a list of allowed names because fuzzy matching doesn't
    # work well at distinguishing them
    filenames: List[str]
    # The definition of the corresponding Justice Counts metric.
    definition: MetricDefinition

    # The dimension by which this metric is disaggregated in this file,
    # e.g. RaceAndEthnicity.
    # (Note that each file can only contain a single disaggregation.)
    disaggregation: Optional[Type[DimensionBase]] = None
    # The name of the column that includes the dimension categories,
    # e.g. `race/ethnicity`.
    disaggregation_column_name: Optional[str] = None

    # Indicates whether this file contains a non-primary aggregation,
    # like gender or race. In this case, the aggregate values don't
    # need to be reported, because they already have been reported
    # on the primary aggregation. If they are reported, they should
    # match the primary aggregation's values.
    supplementary_disaggregation: bool = False


PROSECUTION_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=prosecution.annual_budget,
    ),
    MetricFile(
        filenames=["caseloads"],
        definition=prosecution.caseloads,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filenames=["cases_disposed"],
        definition=prosecution.cases_disposed,
        disaggregation=DispositionType,
        disaggregation_column_name="disposition_type",
    ),
    MetricFile(
        filenames=["cases_referred"],
        definition=prosecution.cases_referred,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filenames=["cases_rejected"],
        definition=prosecution.cases_rejected,
        disaggregation=CaseSeverityType,
        disaggregation_column_name="case_severity",
    ),
    MetricFile(
        filenames=["cases_rejected_by_gender", "cases_rejected_gender"],
        definition=prosecution.cases_rejected,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=["cases_rejected_by_raceethnicity", "cases_rejected_race"],
        definition=prosecution.cases_rejected,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=prosecution.total_staff,
        disaggregation=ProsecutionAndDefenseStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["violations_filed"],
        definition=prosecution.violations,
    ),
]

PRISON_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=prisons.annual_budget,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=prisons.total_staff,
        disaggregation=CorrectionalFacilityStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["readmission_rate"],
        definition=prisons.readmissions,
        disaggregation=ReadmissionType,
        disaggregation_column_name="readmission_type",
    ),
    MetricFile(
        filenames=["admissions"],
        definition=prisons.admissions,
        disaggregation=PrisonPopulationType,
        disaggregation_column_name="admission_type",
    ),
    MetricFile(
        filenames=["average_daily_population"],
        definition=prisons.average_daily_population,
        disaggregation=PrisonPopulationType,
        disaggregation_column_name="population_type",
    ),
    MetricFile(
        filenames=[
            "average_daily_population_by_race/ethnicity",
            "average_daily_population_race",
        ],
        definition=prisons.average_daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=[
            "average_daily_population_by_gender",
            "average_daily_population_gender",
        ],
        definition=prisons.average_daily_population,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=["releases"],
        definition=prisons.releases,
        disaggregation=PrisonReleaseTypes,
        disaggregation_column_name="release_type",
    ),
    MetricFile(
        filenames=["staff_use_of_force_incidents"],
        definition=prisons.staff_use_of_force_incidents,
        disaggregation=CorrectionalFacilityForceType,
        disaggregation_column_name="force_type",
    ),
    MetricFile(
        filenames=["grievances_upheld"],
        definition=prisons.grievances_upheld,
    ),
]

# The `test_metricfile_list` unit test ensures that this dictionary includes
# all metrics registered for each system.
SYSTEM_TO_FILENAME_TO_METRICFILE = {
    schema.System.PRISONS.value: {
        filename: metricfile
        for metricfile in PRISON_METRIC_FILES
        for filename in metricfile.filenames
    },
    schema.System.PROSECUTION.value: {
        filename: metricfile
        for metricfile in PROSECUTION_METRIC_FILES
        for filename in metricfile.filenames
    },
}


def fuzzy_match_against_options(
    analyzer: TextAnalyzer, text: str, options: List[str]
) -> str:
    """Given a piece of input text and a list of options, uses
    fuzzy matching to calculate a match score between the input
    text and each option. Returns the option with the highest
    score, as long as the score is above a cutoff.
    """
    option_to_score = {
        option: fuzz.token_set_ratio(
            analyzer.normalize_text(text, stem_tokens=True),
            analyzer.normalize_text(option, stem_tokens=True),
        )
        for option in options
    }

    best_option = max(option_to_score, key=option_to_score.get)  # type: ignore[arg-type]
    if option_to_score[best_option] < FUZZY_MATCHING_SCORE_CUTOFF:
        raise ValueError(
            "No fuzzy matches found with high enough score. "
            f"Input={text} and options={options}."
        )

    return best_option
