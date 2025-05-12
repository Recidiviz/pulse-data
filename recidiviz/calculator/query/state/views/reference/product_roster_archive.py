# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Archive view of all users that may have access to Polaris products"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRODUCT_ROSTER_ARCHIVE_VIEW_NAME = "product_roster_archive"

PRODUCT_ROSTER_ARCHIVE_DESCRIPTION = (
    """Archive view of data exported from product_roster"""
)

PRODUCT_ROSTER_ARCHIVE_QUERY_TEMPLATE = """
WITH default_fvs AS (
    SELECT
        export_date, email_address,
        -- Extract everything before the ':' to get the fv name
        REGEXP_EXTRACT(default_fv, r'\\s*([^:]+)\\s*') AS fv_name,
        -- Extract everything after the ':' to get the fv value
        REGEXP_EXTRACT(default_fv, r':\\s*(.*)\\s*') AS default_fv_value
    FROM `{project_id}.export_archives.product_roster_archive`
    -- Remove curly braces and double quotes and split on commas
    LEFT JOIN UNNEST(SPLIT(REGEXP_REPLACE(default_feature_variants, {regex_replace_value}, ''))) default_fv
)
, override_fvs AS (
    SELECT
        export_date, email_address,
        REGEXP_EXTRACT(override_fv, r'\\s*([^:]+)\\s*') AS fv_name,
        REGEXP_EXTRACT(override_fv, r':\\s*(.*)\\s*') AS override_fv_value
    FROM `{project_id}.export_archives.product_roster_archive`
    LEFT JOIN UNNEST(SPLIT(REGEXP_REPLACE(override_feature_variants, {regex_replace_value}, ''))) override_fv
)
, all_fvs AS (
    SELECT
        export_date, email_address, fv_name,
        -- We use the postgres sql concat operator in the backend, which effectively overlays all
        -- override values on top of default values. To evaluate the overall value, first check
        -- the override value, then check the default.
        CASE
            -- Since we removed the curly braces, an empty string is the equivalent of an empty JSON object
            WHEN override_fv_value="" THEN TRUE
            WHEN override_fv_value="false" THEN FALSE
            -- Export dates aren't precise because we don't know what time the export occurred and
            -- the user may have recieved the FV at some point during the day, but this is close enough.
            WHEN override_fv_value LIKE "activeDate%" THEN DATE(REGEXP_EXTRACT(override_fv_value, r'activeDate:\\s*(.*)\\s*')) < export_date
            WHEN default_fv_value="" THEN TRUE
            WHEN default_fv_value="false" THEN FALSE
            WHEN default_fv_value LIKE "activeDate%" THEN DATE(REGEXP_EXTRACT(default_fv_value, r'activeDate:\\s*(.*)\\s*')) < export_date
        END AS merged_fv_value
    FROM default_fvs
    FULL OUTER JOIN override_fvs
    USING (export_date, email_address, fv_name)
)
, enabled_fvs AS (
    SELECT export_date, email_address, ARRAY_AGG(fv_name ORDER BY fv_name) AS enabled_fvs
    FROM all_fvs
    WHERE merged_fv_value
    GROUP BY 1, 2
)
SELECT *
FROM `{project_id}.export_archives.product_roster_archive`
LEFT JOIN enabled_fvs
USING (export_date, email_address)
WHERE email_address IS NOT NULL
"""

PRODUCT_ROSTER_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=PRODUCT_ROSTER_ARCHIVE_VIEW_NAME,
    view_query_template=PRODUCT_ROSTER_ARCHIVE_QUERY_TEMPLATE,
    description=PRODUCT_ROSTER_ARCHIVE_DESCRIPTION,
    should_materialize=True,
    regex_replace_value="r'[{{}}\"]'",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_ROSTER_ARCHIVE_VIEW_BUILDER.build_and_print()
