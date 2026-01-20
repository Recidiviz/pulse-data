# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""US_AZ exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.outliers.metric_benchmarks import (
    METRIC_BENCHMARKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_client_events import (
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics import (
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reentry.client import (
    REENTRY_CLIENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.charges_preprocessed import (
    CHARGES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_spans import (
    SENTENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_projected_completion_date_spans import (
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_approaching_acis_or_recidiviz_dtp_request_record import (
    US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_approaching_acis_or_recidiviz_tpr_request_record import (
    US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_az_transfer_to_administrative_supervision_record import (
    US_AZ_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_arson_conviction import (
    VIEW_BUILDER as US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_dangerous_crimes_against_children_conviction import (
    VIEW_BUILDER as US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_domestic_violence_conviction import (
    VIEW_BUILDER as US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_ineligible_offense_conviction_for_admin_supervision import (
    VIEW_BUILDER as US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_sexual_exploitation_of_children_conviction import (
    VIEW_BUILDER as US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_sexual_offense_conviction import (
    VIEW_BUILDER as US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_violent_conviction import (
    VIEW_BUILDER as US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_violent_conviction_unless_assault_or_aggravated_assault_or_robbery_conviction import (
    VIEW_BUILDER as US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.not_serving_flat_sentence import (
    VIEW_BUILDER as US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.not_serving_ineligible_offense_for_admin_supervision import (
    VIEW_BUILDER as US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.only_drug_offense_convictions import (
    VIEW_BUILDER as US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER,
)

# For each US_AZ metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_AZ_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "INSIGHTS": {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        METRIC_BENCHMARKS_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
    },
    "WORKFLOWS_FIRESTORE": {
        US_AZ_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_RECORD_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address,
            },
        },
        US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_DTP_REQUEST_RECORD_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
        },
        US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
        },
        CLIENT_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        RESIDENT_RECORD_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
        },
    },
    "REENTRY": {
        REENTRY_CLIENT_VIEW_BUILDER.address: {
            CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address,
                US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address,
                US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address,
            },
        },
    },
}
