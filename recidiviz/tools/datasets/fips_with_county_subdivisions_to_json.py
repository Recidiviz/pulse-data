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
"""The purpose of this file is to convert a list of dictionaries of all jurisdictions
to a json representation for the frontend."""
import json

from recidiviz.tools.datasets.jurisdictions import (
    get_fips_code_to_jurisdiction_metadata,
)


def main() -> None:
    jurisdictions_json = get_fips_code_to_jurisdiction_metadata()
    json_object = json.dumps(jurisdictions_json, indent=2)
    with open(
        "./recidiviz/common/data_sets/fips_with_county_subdivisions.json",
        "w",
        encoding="utf-8",
    ) as outfile:
        outfile.write(json_object)


if __name__ == "__main__":
    main()
