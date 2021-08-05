# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Define views for collapsing state-aggregate data around fips."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.state_aggregates.combined_state_aggregate import (
    COMBINED_STATE_AGGREGATE_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_DESCRIPTION = """
Select {combined_state_aggregates} and SUM each field around fips, report_date
and aggregation_window. This has the effect of collapsing records around a
single fips in a given report. This is necessary since some reports list data
per facility which all get mapped to the same fips. 
""".format(
    combined_state_aggregates=COMBINED_STATE_AGGREGATE_VIEW_BUILDER.view_id
)

_QUERY_TEMPLATE = """
/*{description}*/

SELECT
  fips,
  report_date,
  aggregation_window,
  SUM(resident_population) AS resident_population,
  SUM(jail_capacity) AS jail_capacity,
  SUM(custodial) AS custodial,
  SUM(admissions) AS admissions,
  SUM(jurisdictional) AS jurisdictional,
  SUM(male) AS male,
  SUM(female) AS female,
  SUM(sentenced) AS sentenced,
  SUM(pretrial) AS pretrial,
  SUM(felony) AS felony,
  SUM(misdemeanor) AS misdemeanor,
  SUM(parole_violators) AS parole_violators,
  SUM(probation_violators) AS probation_violators,
  SUM(technical_parole_violators) AS technical_parole_violators,
  SUM(civil) AS civil,
  SUM(held_for_doc) AS held_for_doc,
  SUM(held_for_federal) AS held_for_federal,
  SUM(total_held_for_other) AS total_held_for_other,
  SUM(held_elsewhere) AS held_elsewhere,
  SUM(jail_capacity_male) AS jail_capacity_male,
  SUM(jail_capacity_female) AS jail_capacity_female,
  SUM(sentenced_male) AS sentenced_male,
  SUM(sentenced_female) AS sentenced_female,
  SUM(pretrial_male) AS pretrial_male,
  SUM(pretrial_female) AS pretrial_female,
  SUM(felony_male) AS felony_male,
  SUM(felony_female) AS felony_female,
  SUM(felony_pretrial) AS felony_pretrial,
  SUM(felony_sentenced) AS felony_sentenced,
  SUM(felony_pretrial_male) AS felony_pretrial_male,
  SUM(felony_pretrial_female) AS felony_pretrial_female,
  SUM(felony_sentenced_male) AS felony_sentenced_male,
  SUM(felony_sentenced_female) AS felony_sentenced_female,
  SUM(misdemeanor_pretrial) AS misdemeanor_pretrial,
  SUM(misdemeanor_sentenced) AS misdemeanor_sentenced,
  SUM(misdemeanor_male) AS misdemeanor_male,
  SUM(misdemeanor_female) AS misdemeanor_female,
  SUM(misdemeanor_pretrial_male) AS misdemeanor_pretrial_male,
  SUM(misdemeanor_pretrial_female) AS misdemeanor_pretrial_female,
  SUM(misdemeanor_sentenced_male) AS misdemeanor_sentenced_male,
  SUM(misdemeanor_sentenced_female) AS misdemeanor_sentenced_female,
  SUM(parole_violators_male) AS parole_violators_male,
  SUM(parole_violators_female) AS parole_violators_female,
  SUM(held_for_doc_male) AS held_for_doc_male,
  SUM(held_for_doc_female) AS held_for_doc_female,
  SUM(held_for_federal_male) AS held_for_federal_male,
  SUM(held_for_federal_female) AS held_for_federal_female,
  SUM(total_held_for_other_male) AS total_held_for_other_male,
  SUM(total_held_for_other_female) AS total_held_for_other_female
FROM
  `{project_id}.{views_dataset}.{combined_state_aggregates}`
GROUP BY
  fips, report_date, aggregation_window
"""

STATE_AGGREGATES_COLLAPSED_TO_FIPS_BUILDER: SimpleBigQueryViewBuilder = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.UNMANAGED_VIEWS_DATASET,
        view_id="state_aggregates_collapsed_to_fips",
        view_query_template=_QUERY_TEMPLATE,
        views_dataset=dataset_config.UNMANAGED_VIEWS_DATASET,
        combined_state_aggregates=COMBINED_STATE_AGGREGATE_VIEW_BUILDER.view_id,
        description=_DESCRIPTION,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_AGGREGATES_COLLAPSED_TO_FIPS_BUILDER.build_and_print()
