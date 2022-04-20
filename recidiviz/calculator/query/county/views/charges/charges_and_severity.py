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
"""Assigns a severity to each Charge record."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.charges.charge_class_severity_ranks import (
    CHARGE_CLASS_SEVERITY_RANKS_VIEW_BUILDER,
)
from recidiviz.common.constants.county.enum_canonical_strings import external_unknown
from recidiviz.persistence.database.schema.county.schema import Charge
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGES_AND_SEVERITY_VIEW_NAME = "charges_and_severity"

CHARGES_AND_SEVERITY_DESCRIPTION = f"""
Assigns a numeric column "severity" to each charge.
Charge class severity is defined in `{dataset_config.VIEWS_DATASET}.charge_class_severity_ranks`.
The lower the number, the more severe the charge class (1 is most severe, 8 is least).
"""

CHARGES_AND_SEVERITY_QUERY_TEMPLATE = """
/*{description}*/
SELECT charge_id, booking_id, class, severity
FROM (
  -- Assign '{external_unknown}' if charge class is NULL.
  -- These will all be lumped into an UNKNOWN category for visualization.
  SELECT charge_id, booking_id, COALESCE(class, '{external_unknown}') AS class
  FROM
  `{project_id}.{base_dataset}.{charge_table}`
) Charge
LEFT JOIN
  `{project_id}.{views_dataset}.{charge_class_severity_ranks_view}` ChargeClassSeverity
ON
  Charge.class = ChargeClassSeverity.charge_class
"""

CHARGES_AND_SEVERITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CHARGES_AND_SEVERITY_VIEW_NAME,
    view_query_template=CHARGES_AND_SEVERITY_QUERY_TEMPLATE,
    description=CHARGES_AND_SEVERITY_DESCRIPTION,
    external_unknown=external_unknown,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
    views_dataset=dataset_config.VIEWS_DATASET,
    charge_table=Charge.__tablename__,
    charge_class_severity_ranks_view=CHARGE_CLASS_SEVERITY_RANKS_VIEW_BUILDER.view_id,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGES_AND_SEVERITY_VIEW_BUILDER.build_and_print()
