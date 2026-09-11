"""create newsflash table

Revision ID: 0001
Revises:
Create Date: 2026-09-11

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "newsflash",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(), nullable=False),
        sa.Column("url", sa.String(512), nullable=False),
        sa.Column("collected_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("source", "url", name="uq_newsflash_source_url"),
    )
    op.create_index("ix_newsflash_published_at", "newsflash", ["published_at"])


def downgrade() -> None:
    op.drop_index("ix_newsflash_published_at", table_name="newsflash")
    op.drop_table("newsflash")
