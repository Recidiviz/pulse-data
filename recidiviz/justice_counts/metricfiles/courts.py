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
"""Metricfile objects used for Courts metrics."""

from recidiviz.justice_counts.dimensions.courts import (
    CourtCaseType,
    CourtReleaseType,
    CourtsCaseSeverityType,
    CourtStaffType,
    SentenceType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import DispositionType
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import courts

COURTS_METRIC_FILES = [
    MetricFile(
        canonical_filename="annual_budget",
        definition=courts.annual_budget,
    ),
    MetricFile(
        canonical_filename="total_staff",
        definition=courts.total_staff,
    ),
    MetricFile(
        canonical_filename="total_staff_by_type",
        definition=courts.total_staff,
        disaggregation=CourtStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        canonical_filename="pretrial_releases",
        definition=courts.pretrial_releases,
    ),
    MetricFile(
        canonical_filename="pretrial_releases_by_type",
        definition=courts.pretrial_releases,
        disaggregation=CourtReleaseType,
        disaggregation_column_name="release_type",
    ),
    MetricFile(
        canonical_filename="sentences_imposed",
        definition=courts.sentences_imposed,
    ),
    MetricFile(
        canonical_filename="sentences_imposed_by_type",
        definition=courts.sentences_imposed,
        disaggregation=SentenceType,
        disaggregation_column_name="sentence_type",
    ),
    MetricFile(
        canonical_filename="sentences_imposed_by_race",
        definition=courts.sentences_imposed,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        canonical_filename="sentences_imposed_by_gender",
        definition=courts.sentences_imposed,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        canonical_filename="cases_filed",
        definition=courts.criminal_case_filings,
    ),
    MetricFile(
        canonical_filename="cases_filed_by_severity",
        definition=courts.criminal_case_filings,
        disaggregation=CourtsCaseSeverityType,
        disaggregation_column_name="severity_type",
    ),
    MetricFile(
        canonical_filename="cases_disposed",
        definition=courts.cases_disposed,
    ),
    MetricFile(
        canonical_filename="cases_disposed_by_type",
        definition=courts.cases_disposed,
        disaggregation=DispositionType,
        disaggregation_column_name="disposition_type",
    ),
    MetricFile(
        canonical_filename="offenses_on_release",
        definition=courts.new_offenses_while_on_pretrial_release,
    ),
    MetricFile(
        canonical_filename="offenses_on_release_by_type",
        definition=courts.new_offenses_while_on_pretrial_release,
        disaggregation=CourtCaseType,
        disaggregation_column_name="case_type",
    ),
    MetricFile(
        canonical_filename="cases_overturned",
        definition=courts.cases_overturned,
    ),
]
