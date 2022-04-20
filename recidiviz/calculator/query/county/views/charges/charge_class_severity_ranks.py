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
"""Creates a View that has a list of charge classes and their severity rank."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.common.constants.county.enum_canonical_strings import (
    charge_class_civil,
    charge_class_felony,
    charge_class_infraction,
    charge_class_misdemeanor,
    charge_class_other,
    charge_class_parole_violation,
    charge_class_probation_violation,
    external_unknown,
)

# Charge classes by severity.
# Must be ranked from highest to lowest severity.
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CHARGE_CLASSES_BY_SEVERITY = [
    charge_class_parole_violation,
    charge_class_probation_violation,
    charge_class_felony,
    charge_class_misdemeanor,
    charge_class_infraction,
    charge_class_other,
    charge_class_civil,
    external_unknown,
]

CHARGE_CLASS_SEVERITY_RANKS_VIEW_NAME = "charge_class_severity_ranks"

CHARGE_CLASS_SEVERITY_RANKS_DESCRIPTION = """
A View of all charge classes and their severity ranks.

Severity is ranked where 0 is most severe, and 7 is least severe.
"""

CHARGE_CLASS_SEVERITY_RANKS_QUERY_TEMPLATE = """
/*{description}*/
SELECT severity, charge_class
FROM
  UNNEST({charge_classes_by_severity_list})
  AS charge_class
WITH OFFSET
  AS severity
ORDER BY severity
"""

CHARGE_CLASS_SEVERITY_RANKS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CHARGE_CLASS_SEVERITY_RANKS_VIEW_NAME,
    view_query_template=CHARGE_CLASS_SEVERITY_RANKS_QUERY_TEMPLATE,
    description=CHARGE_CLASS_SEVERITY_RANKS_DESCRIPTION,
    charge_classes_by_severity_list=str(CHARGE_CLASSES_BY_SEVERITY),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CHARGE_CLASS_SEVERITY_RANKS_VIEW_BUILDER.build_and_print()
