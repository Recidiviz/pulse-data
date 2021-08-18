"""Script to generate an alembic migration for deprecating a field on an entity for a
state_code by setting the value of all instances to null.

Usage:
python -m recidiviz.tools.create_field_deprecation_migration \
    --primary-table <primary table name> --column <column name> \
    --state-code-to-deprecate <state code to deprecate> \
     --migration-name <name for migration>

Example:
python -m recidiviz.tools.create_field_deprecation_migration \
    --primary-table state_incarceration_period --column incarceration_type \
    --state-code-to-deprecate US_XX --state-code-to-deprecate US_YY \
    --migration-name drop_incarceration_type_us_xx_us_yy
"""
import argparse
import os
from typing import List

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tools.utils.migration_script_helpers import (
    PATH_TO_MIGRATIONS_DIRECTORY,
    create_new_empty_migration_and_return_filename,
    get_migration_header_section,
    path_to_versions_directory,
)

_PATH_TO_BODY_SECTION_TEMPLATE = os.path.join(
    PATH_TO_MIGRATIONS_DIRECTORY, "field_deprecation_migration_template.txt"
)


def _get_migration_body_section(
    primary_table_name: str,
    column_name: str,
    state_codes_to_deprecate: List[str],
) -> str:
    """Returns string of body section of field deprecation migration by interpolating
    provided values into field deprecation migration template
    """
    with open(_PATH_TO_BODY_SECTION_TEMPLATE, "r") as template_file:
        template = template_file.read()
    return template.format(
        primary_table=primary_table_name,
        column=column_name,
        state_codes_to_deprecate=", ".join(
            [f"'{state_code}'" for state_code in state_codes_to_deprecate]
        ),
    )


def _create_parser() -> argparse.ArgumentParser:
    """Creates an argparser for the script."""
    parser = argparse.ArgumentParser(description="Autogenerate local migration.")
    parser.add_argument(
        "--migration-name",
        type=str,
        help="String message passed to alembic to apply to the revision.",
        required=True,
    )
    parser.add_argument(
        "--primary-table",
        type=str,
        help="Name of the primary table where the migration will be run.",
        required=True,
    )
    parser.add_argument(
        "--column",
        type=str,
        help="Name of the column whose values will be updated to be null.",
        required=True,
    )
    parser.add_argument(
        "--state-code-to-deprecate",
        type=str,
        help="The state_code being deprecated. Can be repeated to deprecate the field for multiple states.",
        required=True,
        action="append",
        default=[],
    )

    return parser


def main() -> None:
    """Implements the main function of the script."""
    parser = _create_parser()
    args = parser.parse_args()

    # NOTE: We use prints instead of logging because the call out to alembic
    # does something to mess with our logging levels.
    migration_filename = create_new_empty_migration_and_return_filename(
        SchemaType.STATE, args.migration_name
    )
    migration_filepath = os.path.join(
        path_to_versions_directory(SchemaType.STATE), migration_filename
    )
    header_section = get_migration_header_section(migration_filepath)
    body_section = _get_migration_body_section(
        args.primary_table,
        args.column,
        args.state_code_to_deprecate,
    )
    file_content = "{}\n{}".format(header_section, body_section)

    with open(migration_filepath, "w") as migration_file:
        migration_file.write(file_content)

    print(f"Successfully generated migration: {migration_filename}")


if __name__ == "__main__":
    main()
