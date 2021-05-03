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

import recidiviz
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.tools.docs.summary_file_generator import update_summary_file
from recidiviz.tools.docs.utils import persist_file_contents

ENTITY_DOCS_ROOT = os.path.join(
    os.path.dirname(recidiviz.__file__), "..", "docs", "schema"
)


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

    anything_modified = False
    for t in StateBase.metadata.sorted_tables:
        documentation = f"## {t.name}\n\n"
        documentation += (
            f"{t.comment}\n\n"
            if t.comment
            else "TODO(#7120): FILL IN THIS DESCRIPTION\n\n"
        )
        documentation += f"{_get_fields(t.columns)}\n\n"

        markdown_file_path = os.path.join(ENTITY_DOCS_ROOT, f"{t.name}.md")
        anything_modified |= persist_file_contents(documentation, markdown_file_path)

    return anything_modified


def main() -> int:
    def _generate_entity_documentation_summary() -> str:
        list_of_tables: str = "\n".join(
            sorted(
                f"\t - [{entity_table}](schema/{entity_table}.md)"
                for entity_table in StateBase.metadata.sorted_tables
            )
        )
        list_of_tables += "\n"
        return list_of_tables

    def _generate_summary_strings() -> List[str]:
        entity_documentation_summary = ["## Schema Catalog\n\n"]
        entity_documentation_summary.extend(["- entities\n"])
        entity_documentation_summary.extend(_generate_entity_documentation_summary())
        return entity_documentation_summary

    # TODO(#7083): Add check that schema.py has been modified before generating entity documentation.
    modified = False
    modified |= generate_entity_documentation()
    if modified:
        update_summary_file(
            _generate_summary_strings(),
            "## Schema Catalog",
        )
    return 1 if modified else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    sys.exit(main())
