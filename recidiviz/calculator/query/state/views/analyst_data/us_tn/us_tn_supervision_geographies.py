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
"""Metric capturing proportion of supervision periods successfully terminated
in a month via a granted early discharge"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUPERVISION_GEOGRAPHIES_VIEW_NAME = "us_tn_supervision_geographies"

US_TN_SUPERVISION_GEOGRAPHIES_VIEW_DESCRIPTION = "Table mapping supervision location codes to district, division, office, and county in TN"

US_TN_SUPERVISION_GEOGRAPHIES_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        SiteID AS supervision_office, 
        SiteName AS supervision_office_name,
        r.District AS district,
        r.Region AS region,
        c.county_name,
        fips,
    FROM 
        `{project_id}.us_tn_raw_data_up_to_date_views.Site_latest` r
    LEFT JOIN 
        `{project_id}.{static_reference_dataset}.supervision_district_office_to_county` o
    ON 
        r.SiteName = o.office
    LEFT JOIN
        `{project_id}.external_reference.county_fips` c
    ON
        o.county = c.county_name
    WHERE 
        SiteType IN ("PR", "PX", "PA")
    """

US_TN_SUPERVISION_GEOGRAPHIES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_TN_SUPERVISION_GEOGRAPHIES_VIEW_NAME,
    view_query_template=US_TN_SUPERVISION_GEOGRAPHIES_QUERY_TEMPLATE,
    description=US_TN_SUPERVISION_GEOGRAPHIES_VIEW_DESCRIPTION,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_GEOGRAPHIES_VIEW_BUILDER.build_and_print()
