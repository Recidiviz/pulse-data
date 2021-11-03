"""Script to generate an alembic migration for adding a new value to an enum

Usage:
python -m recidiviz.tools.create_enum_migration --schema <schema name> \
    --primary-table <primary table name> --enum-name <enum type name> \
    --column <column name> --new-enum-value <new value> --remove-enum-value \
    <remove value> --migration-name <name for migration>

Example:
python -m recidiviz.tools.create_enum_migration --schema STATE \
    --primary-table state_person_race --enum-name race --column race \
    --new-enum-value LIZARD --new-enum-value BIRD --remove-enum-value OTHER \
    --migration-name add_race_lizard_bird
"""
import argparse
import os
import sys
from typing import List

from pytest_alembic import runner
from pytest_alembic.config import Config
from sqlalchemy import create_engine

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.tools.utils.migration_script_helpers import (
    PATH_TO_MIGRATIONS_DIRECTORY,
    create_new_empty_migration_and_return_filename,
    get_migration_header_section,
    path_to_versions_directory,
)
from recidiviz.utils.string import StrictStringFormatter

_PATH_TO_BODY_SECTION_TEMPLATE = os.path.join(
    PATH_TO_MIGRATIONS_DIRECTORY, "enum_migration_template.txt"
)


def _get_migration_body_section(
    primary_table_name: str,
    enum_name: str,
    column_name: str,
    old_values: List[str],
    new_values: List[str],
) -> str:
    """Returns string of body section of enum migration by interpolating
    provided values into enum migration template
    """
    with open(_PATH_TO_BODY_SECTION_TEMPLATE, "r", encoding="utf-8") as template_file:
        template = template_file.read()
    return StrictStringFormatter().format(
        template,
        primary_table=primary_table_name,
        enum_name=enum_name,
        column=column_name,
        old_values=old_values,
        new_values=new_values,
    )


def _get_old_enum_values(schema_type: SchemaType, enum_name: str) -> List[str]:
    """Fetches the current enum values for the given schema and enum name."""
    # Setup temp pg database
    db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
    database_key = SQLAlchemyDatabaseKey.canonical_for_schema(schema_type)
    overridden_env_vars = (
        local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
    )
    engine = create_engine(local_postgres_helpers.postgres_db_url_from_env_vars())

    try:
        # Fetch enums
        default_config = Config.from_raw_config(
            {
                "file": database_key.alembic_file,
                "script_location": database_key.migrations_location,
            }
        )
        with runner(default_config, engine) as r:
            r.migrate_up_to("head")
        conn = engine.connect()
        rows = conn.execute(
            f"""
        SELECT e.enumlabel as enum_value
        FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE
            n.nspname = 'public'
            AND t.typname = '{enum_name}';
        """
        )
        enums = [row[0] for row in rows]
    finally:
        # Teardown temp pg database
        local_postgres_helpers.restore_local_env_vars(overridden_env_vars)
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(db_dir)

    return enums


def _create_parser() -> argparse.ArgumentParser:
    """Creates an argparser for the script."""
    parser = argparse.ArgumentParser(description="Autogenerate local migration.")
    parser.add_argument(
        "--schema",
        type=SchemaType,
        choices=list(SchemaType),
        help="Specifies which schema to generate migrations for.",
        required=True,
    )
    parser.add_argument(
        "--migration-name",
        type=str,
        help="String message passed to alembic to apply to the revision.",
        required=True,
    )
    parser.add_argument(
        "--primary-table",
        type=str,
        help="Name of the primary table where the enum migration will be run.",
        required=True,
    )
    parser.add_argument(
        "--enum-name",
        type=str,
        help="Postgres name of the enum whose values will be updated.",
        required=True,
    )
    parser.add_argument(
        "--column",
        type=str,
        help="Name of the column whose values will be updated.",
        required=True,
    )
    parser.add_argument(
        "--new-enum-value",
        type=str,
        action="append",
        default=[],
        help="Name of the newly added enum value. Can be repeated to add multiple values.",
    )
    parser.add_argument(
        "--remove-enum-value",
        type=str,
        action="append",
        default=[],
        help="Name of an enum value to remove. Can be repeated to remove multiple values.",
    )
    return parser


def main() -> None:
    """Implements the main function of the script."""
    parser = _create_parser()
    args = parser.parse_args()

    # NOTE: We use prints instead of logging because the call out to alembic
    # does something to mess with our logging levels.
    print("Generating old enum values...")
    old_enum_values = _get_old_enum_values(args.schema, args.enum_name)
    print("Found old enum values.")
    for val in args.new_enum_value:
        if val in old_enum_values:
            print(
                f"Enum value [{val}] was set to be added but is already present "
                f"for enum [{args.enum_name}]."
            )
            sys.exit(1)
    for val in args.remove_enum_value:
        if val not in old_enum_values:
            print(
                f"Enum value [{val}] was set to be removed but is not present "
                f"for enum [{args.enum_name}]."
            )
            sys.exit(1)
        if val in args.new_enum_value:
            print(
                f"Enum value [{val}] was set to be both added and removed. Terminating."
            )
            sys.exit(1)

    new_enum_values = [
        val
        for val in [*old_enum_values, *args.new_enum_value]
        if val not in args.remove_enum_value
    ]

    migration_filename = create_new_empty_migration_and_return_filename(
        args.schema, args.migration_name
    )
    migration_filepath = os.path.join(
        path_to_versions_directory(args.schema), migration_filename
    )
    header_section = get_migration_header_section(migration_filepath)
    body_section = _get_migration_body_section(
        args.primary_table,
        args.enum_name,
        args.column,
        old_enum_values,
        new_enum_values,
    )
    file_content = f"{header_section}\n{body_section}"

    with open(migration_filepath, "w", encoding="utf-8") as migration_file:
        migration_file.write(file_content)

    print(f"Successfully generated migration: {migration_filename}")


if __name__ == "__main__":
    main()
