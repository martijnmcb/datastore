"""add report pivot fields

Revision ID: 9b1c2d3e4f5a
Revises: e2e24a977632
Create Date: 2026-01-10 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "9b1c2d3e4f5a"
down_revision = "e2e24a977632"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = inspector.get_table_names()

    if "report_templates" not in tables:
        op.create_table(
            "report_templates",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("name", sa.String(255), nullable=False),
            sa.Column("dataset", sa.String(120), nullable=False, server_default="rgritten"),
            sa.Column("row_limit", sa.Integer(), nullable=False, server_default="1000"),
            sa.Column("include_fields", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("filter_fields", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("sort_fields", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("group_fields", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("pivot_enabled", sa.Boolean(), nullable=False, server_default="0"),
            sa.Column("pivot_row_fields", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("pivot_col_field", sa.String(255), nullable=True),
            sa.Column("pivot_values", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )
        with op.batch_alter_table("report_templates") as batch_op:
            batch_op.alter_column("dataset", server_default=None)
            batch_op.alter_column("row_limit", server_default=None)
            batch_op.alter_column("include_fields", server_default=None)
            batch_op.alter_column("filter_fields", server_default=None)
            batch_op.alter_column("sort_fields", server_default=None)
            batch_op.alter_column("group_fields", server_default=None)
            batch_op.alter_column("pivot_enabled", server_default=None)
            batch_op.alter_column("pivot_row_fields", server_default=None)
            batch_op.alter_column("pivot_values", server_default=None)
            batch_op.alter_column("created_at", server_default=None)
    else:
        op.add_column(
            "report_templates",
            sa.Column("pivot_enabled", sa.Boolean(), nullable=False, server_default="0"),
        )
        op.add_column(
            "report_templates",
            sa.Column("pivot_row_fields", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        )
        op.add_column(
            "report_templates",
            sa.Column("pivot_col_field", sa.String(255), nullable=True),
        )
        op.add_column(
            "report_templates",
            sa.Column("pivot_values", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        )
        with op.batch_alter_table("report_templates") as batch_op:
            batch_op.alter_column("pivot_enabled", server_default=None)
            batch_op.alter_column("pivot_row_fields", server_default=None)
            batch_op.alter_column("pivot_values", server_default=None)


def downgrade():
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = inspector.get_table_names()
    if "report_templates" in tables:
        cols = [c["name"] for c in inspector.get_columns("report_templates")]
        for col in ("pivot_values", "pivot_col_field", "pivot_row_fields", "pivot_enabled"):
            if col in cols:
                op.drop_column("report_templates", col)
