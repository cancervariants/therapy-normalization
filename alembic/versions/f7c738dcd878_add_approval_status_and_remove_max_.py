"""Add approval_status and remove max_phase, withdrawn_flag in therapies

Revision ID: f7c738dcd878
Revises: 05a8e608b181
Create Date: 2020-11-03 11:29:03.916207

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f7c738dcd878'
down_revision = '05a8e608b181'
branch_labels = None
depends_on = None


def upgrade():
    """Add approval_status and remove max_phase, withdrawn_flag in therapies"""
    # TODO: Fix SAWarning: Skipped unsupported reflection of expression-based
    #  index lower_therapies_label, lower_therapies_c_id
    with op.batch_alter_table('therapies', schema=None) as batch_op:
        batch_op.add_column(sa.Column('approval_status',
                                      sa.String(), nullable=True))
        batch_op.create_index('lower_therapies_c_id',
                              [sa.text('lower(concept_id)')], unique=False)
        batch_op.create_index('lower_therapies_label',
                              [sa.text('lower(label)')], unique=False)
        batch_op.drop_column('withdrawn_flag')
        batch_op.drop_column('max_phase')


def downgrade():
    """Drop approval_status and add max_phase, withdrawn_flag in therapies"""
    with op.batch_alter_table('therapies', schema=None) as batch_op:
        batch_op.add_column(sa.Column('max_phase',
                                      sa.INTEGER(), nullable=True))
        batch_op.add_column(sa.Column('withdrawn_flag',
                                      sa.BOOLEAN(), nullable=True))
        batch_op.drop_index('lower_therapies_label')
        batch_op.drop_index('lower_therapies_c_id')
        batch_op.drop_column('approval_status')
