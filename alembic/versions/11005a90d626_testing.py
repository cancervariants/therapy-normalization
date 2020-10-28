"""testing

Revision ID: 11005a90d626
Revises:
Create Date: 2020-10-28 11:27:19.314735

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '11005a90d626'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Upgrade db with new table"""
    # op.execute("create schema therapies_schema")
    op.create_table(
        'test',
        sa.Column('IdNo', sa.Integer, primary_key=True),
        sa.Column('Nombre', sa.String)
    )


def downgrade():
    """Drop new table"""
    op.drop_table('test')
