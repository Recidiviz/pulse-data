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
"""Creates the view builder and view for fetching contact info (emails).

TODO(#7564): In the long-term this info should be ingested into the state schema.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_CONTACT_INFO_QUERY_TEMPLATE = """
WITH phone_numbers AS (
  SELECT
    docno,
    phonenumber AS phone_number
  FROM (
    SELECT
      offender.offendernumber AS docno,
      phonenumber,
      ROW_NUMBER() OVER (PARTITION BY offender.offendernumber ORDER BY numbers.insdate DESC) AS rn
    FROM
      `{project_id}.us_id_raw_data_up_to_date_views.cis_offender_latest` offender
    INNER JOIN
      `{project_id}.us_id_raw_data_up_to_date_views.cis_personphonenumber_latest` person_number
    ON
      (offender.id = person_number.personid)
    INNER JOIN
      `{project_id}.us_id_raw_data_up_to_date_views.cis_phonenumber_latest` numbers
    ON
      (numbers.id = person_number.phonenumberid) )
  WHERE
    rn = 1
)
SELECT
  'US_ID' AS state_code,
  person_external_id,
  email_address,
  phone_number
FROM (
    SELECT offenders.offendernumber AS person_external_id,
    email AS email_address,
    ROW_NUMBER() OVER (PARTITION BY offenders.offendernumber ORDER BY emails.insdate DESC) AS rn
    FROM `{project_id}.us_id_raw_data_up_to_date_views.cis_offender_latest` offenders
    LEFT JOIN
    `{project_id}.us_id_raw_data_up_to_date_views.cis_personemailaddress_latest` emails
    ON (emails.personid = offenders.id)
    WHERE emails.iscurrent = 'T'
    QUALIFY rn = 1
)
LEFT JOIN
  phone_numbers
ON person_external_id = phone_numbers.docno
"""

CLIENT_CONTACT_INFO_DESCRIPTION = """
Provides an association between people on supervision and their contact info.
 
Currently only generates data for Idaho and only contains email addresses."""

CLIENT_CONTACT_INFO_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="client_contact_info",
    description=CLIENT_CONTACT_INFO_DESCRIPTION,
    view_query_template=CLIENT_CONTACT_INFO_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_CONTACT_INFO_VIEW_BUILDER.build_and_print()
