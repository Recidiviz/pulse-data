# pylint: skip-file
"""add normalized unique index to datapoint constraint

Revision ID: 1d55ae322ca7
Revises: f190ada183e4
Create Date: 2025-03-25 15:16:21.693975

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1d55ae322ca7"
down_revision = "f190ada183e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("unique_datapoint", "datapoint", type_="unique")
    op.execute(
        """
        CREATE UNIQUE INDEX unique_report_datapoint_normalized
        ON datapoint (
            report_id,
            start_date,
            end_date,
            COALESCE(dimension_identifier_to_member, '{}'::jsonb),
            context_key,
            includes_excludes_key,
            metric_definition_key,
            source_id
        );
    """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS unique_report_datapoint_normalized;
    """
    )
    op.create_unique_constraint(
        "unique_datapoint",
        "datapoint",
        [
            "report_id",
            "start_date",
            "end_date",
            "dimension_identifier_to_member",
            "context_key",
            "includes_excludes_key",
            "metric_definition_key",
            "source_id",
        ],
    )
