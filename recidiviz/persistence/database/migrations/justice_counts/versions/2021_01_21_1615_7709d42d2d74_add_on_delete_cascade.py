# pylint: skip-file
"""add_on_delete_cascade

Revision ID: 7709d42d2d74
Revises: 7c6692eb9f89
Create Date: 2021-01-21 16:15:50.029548

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7709d42d2d74'
down_revision = '7c6692eb9f89'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('report_table_instance_report_id_fkey', 'report_table_instance', type_='foreignkey')
    op.create_foreign_key('report_table_instance_report_id_fkey', 'report_table_instance', 'report',
                    ['report_id'], ['id'], ondelete='CASCADE')
    op.drop_constraint('cell_report_table_instance_id_fkey', 'cell', type_='foreignkey')
    op.create_foreign_key('cell_report_table_instance_id_fkey', 'cell', 'report_table_instance',
                          ['report_table_instance_id'], ['id'], ondelete='CASCADE')


def downgrade():
    op.drop_constraint('report_table_instance_report_id_fkey', 'report_table_instance', type_='foreignkey')
    op.create_foreign_key('report_table_instance_report_id_fkey', 'report_table_instance', 'report',
                          ['report_id'], ['id'])
    op.drop_constraint('cell_report_table_instance_id_fkey', 'cell', type_='foreignkey')
    op.create_foreign_key('cell_report_table_instance_id_fkey', 'cell', 'report_table_instance',
                          ['report_table_instance_id'], ['id'])
