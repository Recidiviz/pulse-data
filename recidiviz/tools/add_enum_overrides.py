# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Script for automating the addition of enum overrides to individual roster
scrapers.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.add_enum_overrides
"""

import argparse
import logging
import pandas as pd


def add_enum_override(
    scraper_name: str, from_field: str, enum_string: str, action: str, mapped_enum: str
) -> None:
    """Add one enum mapping to a single scraper file."""
    logging.info(scraper_name)
    scraper_file = (
        f"recidiviz/ingest/scrape/regions/{scraper_name}/{scraper_name}_scraper.py"
    )
    with open(scraper_file) as file:
        lines = file.readlines()
    index = next(
        (
            i
            for i, line in enumerate(lines)
            if "return overrides_builder.build()" in line
        ),
        None,
    )
    if index and lines[index - 1].isspace():
        index -= 1
    action = action.strip()
    if action == "add":
        if from_field == mapped_enum.split(".")[0]:
            new_enum_override = (
                f"        overrides_builder.add('{enum_string}', {mapped_enum})\n"
            )
        else:
            new_enum_override = f"        overrides_builder.add('{enum_string}', {mapped_enum}, {from_field})\n"
    elif action == "ignore":
        new_enum_override = (
            f"        overrides_builder.ignore('{enum_string}', {from_field})\n"
        )
    else:
        raise ValueError(f"Unknown action [{action}], expected 'add' or 'ignore'.")
    if index:
        lines.insert(index, new_enum_override)
    else:
        override_builder = [
            "    def get_enum_overrides(self) -> EnumOverrides:\n",
            "        overrides_builder = super().get_enum_overrides().to_builder()\n",
            new_enum_override,
            "        return overrides_builder.build()\n",
        ]
        lines.extend(override_builder)
    with open(scraper_file, "w") as file:
        file.writelines(lines)


def main(df: pd.DataFrame) -> None:
    for _, (
        scraper_name,
        from_field,
        enum_string,
        action,
        mapped_enum,
        everyone,
    ) in df.iterrows():
        if everyone:
            logging.warning(
                "Update the default map for %s to map '%s' to %s.",
                from_field,
                enum_string,
                mapped_enum,
            )
        else:
            add_enum_override(
                scraper_name, from_field, enum_string, action, mapped_enum
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path")
    args = parser.parse_args()
    enum_df = pd.read_csv(args.file_path)
    usecols = [
        "scraper_name",
        "from_field",
        "enum_string",
        "action",
        "mapped_enum",
        "everyone",
    ]
    enum_df = enum_df[enum_df["implemented"] < 1][usecols]
    main(enum_df)
