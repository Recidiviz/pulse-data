# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Supervision termination data with corresponding judicial district for the public dashboard"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.public_dashboard.utils import (
    spotlight_age_buckets,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_NAME = (
    "supervision_terminations_for_spotlight"
)

SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_DESCRIPTION = """Supervision termination data with corresponding judicial district for the public dashboard."""

SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_QUERY_TEMPLATE = f"""
    SELECT
        sup_term.state_code,
        sup_term.person_id,
        year,
        month,
        termination_reason,
        supervision_type,
        supervising_district_external_id,
        sent.judicial_district AS judicial_district_code,
        gender,
        prioritized_race_or_ethnicity,
        {{age_bucket}},
    FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_termination_metrics_materialized` sup_term
    -- Join all sentences that were active on the supervision termination date. Unnest
    -- the sentence_imposed_group_id_array in a sub-query in case there are no
    -- overlapping sentence spans to avoid dropping supervision termination rows
    LEFT JOIN (
      SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        judicial_district,
        effective_date,
        date_imposed,
      FROM `{{project_id}}.{{sessions_dataset}}.sentence_spans_materialized` sent_spans,
      UNNEST (sentence_imposed_group_id_array) sentence_imposed_group_id
      LEFT JOIN `{{project_id}}.{{sessions_dataset}}.sentence_imposed_group_summary_materialized` sent_imp
      USING (state_code, person_id, sentence_imposed_group_id)
    ) sent
        ON sup_term.state_code = sent.state_code
        AND sup_term.person_id = sent.person_id
        AND sup_term.termination_date BETWEEN sent.start_date AND {bq_utils.nonnull_end_date_clause("sent.end_date_exclusive")}
    WHERE {{thirty_six_month_filter}} AND termination_reason NOT IN (
        'DEATH',
        'EXTERNAL_UNKNOWN',
        'INTERNAL_UNKNOWN',
        'RETURN_FROM_ABSCONSION',
        'SUSPENSION',
        'TRANSFER_WITHIN_STATE',
        'TRANSFER_TO_OTHER_JURISDICTION')
    -- Pick the judicial district code from the sentence that started the closest to
    -- the supervision termination date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id, termination_date
        ORDER BY effective_date DESC, date_imposed DESC, judicial_district_code
    ) = 1
    """

SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    age_bucket=spotlight_age_buckets(),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    clustering_fields=["state_code"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.build_and_print()
