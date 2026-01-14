"""add data refresh config

Revision ID: 7f3a6e4bbd6c
Revises: e2e24a977632
Create Date: 2026-01-05 10:44:54.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import Boolean, String, Integer, DateTime, func


# revision identifiers, used by Alembic.
revision = '7f3a6e4bbd6c'
down_revision = 'e2e24a977632'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'data_refresh_config',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default=sa.text('0')),
        sa.Column('run_time', sa.String(length=10), nullable=False, server_default='02:00'),
        sa.Column('profile_id', sa.Integer(), sa.ForeignKey('connection_profiles.id'), nullable=True),
        sa.Column('chunk_size', sa.Integer(), nullable=False, server_default='1000'),
        sa.Column('min_ritdatum', sa.String(length=20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=func.now()),
    )
    cfg = table(
        'data_refresh_config',
        column('id', Integer),
        column('enabled', Boolean),
        column('run_time', String),
        column('chunk_size', Integer),
        column('created_at', DateTime),
    )
    op.execute(cfg.insert().values(id=1, enabled=False, run_time="02:00", chunk_size=1000))


def downgrade():
    op.drop_table('data_refresh_config')
