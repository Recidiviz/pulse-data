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
import csv
import json
import os
from typing import Dict, List, Optional

from recidiviz.cloud_sql.gcs_import_to_cloud_sql import (
    parse_exported_json_row_from_bigquery,
)
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.persistence.database.schema.outliers.schema import OutliersBase
from recidiviz.tools.insights import fixtures as insights_json_fixtures
from recidiviz.tools.outliers import fixtures as outliers_csv_fixtures


def load_model_from_csv_fixture(
    model: OutliersBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.csv" if filename is None else filename
    fixture_path = os.path.join(
        os.path.dirname(outliers_csv_fixtures.__file__), filename
    )
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        reader = csv.DictReader(fixture_file)
        for row in reader:
            for k, v in row.items():
                if k == "has_seen_onboarding" and v != "":
                    row["has_seen_onboarding"] = json.loads(row["has_seen_onboarding"])
                if v == "True":
                    row[k] = True
                if v == "False":
                    row[k] = False
                if v == "":
                    row[k] = None
            results.append(row)

    return results


def load_model_from_json_fixture(
    model: InsightsBase, filename: Optional[str] = None
) -> List[Dict]:
    filename = f"{model.__tablename__}.json" if filename is None else filename
    fixture_path = os.path.join(
        os.path.dirname(insights_json_fixtures.__file__), filename
    )
    results = []
    with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
        for row in fixture_file:
            json_obj = json.loads(row)
            flattened_json = parse_exported_json_row_from_bigquery(json_obj, model)
            results.append(flattened_json)

    return results
