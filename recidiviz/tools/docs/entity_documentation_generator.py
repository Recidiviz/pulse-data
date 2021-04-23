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
# pylint: disable=unused-import

"""A script which will be called using a pre-commit githook to generate entity documentation.

Can be run on-demand via:
    $ pipenv run python -m recidiviz.tools.docs.entity_documentation_generator
"""

import logging
import os
import sys

from typing import List

import sqlalchemy
from pytablewriter import MarkdownTableWriter

from recidiviz.persistence.database.schema.state import (
    schema as state_schema,
)
from recidiviz.persistence.database.base_schema import StateBase

ENTITY_DOCS_ROOT = "docs/schema"


def generate_entity_documentation() -> bool:
    """
    Parses `persistence/entity/state/entities_docs.yaml` to produce documentation. Overwrites or creates the
    corresponding Markdown file.

    Returns True if files were modified, False otherwise.
    """

    def _get_fields(fields: List[sqlalchemy.Column]) -> str:
        """Returns a table of the entity's fields and their corresponding descriptions."""
        if fields is None:
            return "No Fields"
        if not fields:
            return "<No columns>"

        table_matrix = [
            [
                field.name,
                field.comment
                # TODO(#7120) once all comments are added to schema.py, throw an error if comment is None.
                if field.comment is not None
                else "TODO(#7120): FILL IN THIS DESCRIPTION",
                f"ENUM: <br />{'<br />'.join([f'{e}' for e in field.type.enums])}"
                if hasattr(field.type, "enums")
                else field.type.python_type.__name__.upper(),
            ]
            for field in fields
        ]

        writer = MarkdownTableWriter(
            headers=[
                "Entity Field",
                "Entity Description",
                "Entity Type",
            ],
            value_matrix=table_matrix,
            margin=1,
        )
        return writer.dumps()

    documentation = "# Entity Documentation\n\n"
    for t in StateBase.metadata.sorted_tables:
        documentation += f"## {t.name}\n\n"
        documentation += (
            f"{t.comment}\n\n"
            if t.comment
            else "TODO(#7120): FILL IN THIS DESCRIPTION\n\n"
        )
        documentation += f"{_get_fields(t.columns)}\n\n"

    markdown_file_path = os.path.join(ENTITY_DOCS_ROOT, "entities.md")

    anything_modified = False

    if os.path.exists(markdown_file_path):
        prior_documentation = ""
        with open(markdown_file_path, "r") as raw_data_md_file:
            prior_documentation = raw_data_md_file.read()

        if prior_documentation != documentation:
            with open(markdown_file_path, "w") as raw_data_md_file:
                raw_data_md_file.write(documentation)
                anything_modified = True

    return anything_modified


def main() -> int:
    # TODO(#7083): Add check that schema.py has been modified before generating entity documentation.
    modified = False
    modified |= generate_entity_documentation()
    return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
