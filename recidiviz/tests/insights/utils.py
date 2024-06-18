#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""This class implements tests for the OutliersQuerier class"""
import json
import os
from typing import Dict, List, Optional

from recidiviz.cloud_sql.gcs_import_to_cloud_sql import (
    get_non_identity_columns_from_model,
    parse_exported_json_row_from_bigquery,
)
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.tools.insights import fixtures as insights_json_fixtures


def load_model_from_json_fixture(
    model: InsightsBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.json" if filename is None else filename
    fixture_path = os.path.join(
        os.path.dirname(insights_json_fixtures.__file__), filename
    )
    results = []
    model_columns = get_non_identity_columns_from_model(model)
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        for row in fixture_file:
            json_obj = json.loads(row)
            flattened_json = parse_exported_json_row_from_bigquery(
                json_obj, model, model_columns
            )
            results.append(flattened_json)

    return results
