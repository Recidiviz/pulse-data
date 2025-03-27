# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""This module has functionality to randomize fixture data."""
# TODO(#40159) Update randomization capabilities

import string

# TODO(#40159) Faker has functionality to generate the things we're using
# numpy for here. We should strive to use Faker first.
import numpy
from faker import Faker

from recidiviz.ingest.direct.raw_data.raw_file_configs import RawTableColumnInfo

Faker.seed(0)
FAKE = Faker(locale=["en-US"])


def randomize_value(
    value: str, column_info: RawTableColumnInfo, datetime_format: str
) -> str:
    """For each character in a string, checks whether the character is a number or letter and replaces it with a random
    matching type character."""
    # TODO(#12179) Improve and unit test randomization options by specifying pii_type.
    randomized_value = ""
    if column_info.is_datetime:
        randomized_value = FAKE.date(pattern=datetime_format)
    elif "name" in column_info.name.lower():
        first_middle_name_strs = {"first", "f", "middle", "m"}
        if any(x in column_info.name for x in first_middle_name_strs):
            randomized_value = FAKE.first_name_nonbinary() + str(FAKE.random_number(2))
        surname_strs = {"surname", "last", "l", "sur"}
        if any(x in column_info.name for x in surname_strs):
            randomized_value = FAKE.last_name() + str(FAKE.random_number(2))
    else:
        randomized_value = ""
        for character in value:
            if character.isnumeric():
                randomized_value += str(numpy.random.randint(1, 9, 1)[0])
            if character.isalpha():
                randomized_value += numpy.random.choice(
                    list(string.ascii_uppercase), size=1
                )[0]

    print(
        f"Randomizing value from column '{column_info.name}': {value} -> {randomized_value}"
    )
    return randomized_value
