# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Example script used to test validate source visibility functionality."""

import requests

from recidiviz.common.date import current_date_us_eastern_in_iso
from recidiviz.utils import secrets


def main(usefile: bool) -> None:
    params = secrets.get_secret("params")

    request = requests.get("https://www.google.com", params, timeout=60)
    request.raise_for_status()

    if usefile:
        import tempfile  # pylint: disable=import-outside-toplevel

        with tempfile.TemporaryFile() as file:
            file.write(request.content)

    else:
        print(current_date_us_eastern_in_iso())
        print(request.text)


if __name__ == "__main__":
    main(True)
