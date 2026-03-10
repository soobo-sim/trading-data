"""bf_candles 테이블 생성

Revision ID: 001
Revises: 
Create Date: 2026-03-10

bf_candles: BitFlyer OHLCV 캔들 (1H / 4H)
ck_candles: coincheck-trader 마이그레이션 015에서 이미 생성됨 → 여기서는 생성 불필요
"""
from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bf_candles",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("product_code", sa.String(20), nullable=False),
        sa.Column("timeframe", sa.String(5), nullable=False),
        sa.Column("open_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("close_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.Numeric(18, 8), nullable=False, server_default="0"),
        sa.Column("tick_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_complete", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("product_code", "timeframe", "open_time", name="uq_bf_candles_pc_tf_open"),
    )
    op.create_index("idx_bf_candles_lookup", "bf_candles", ["product_code", "timeframe", "open_time"])
    op.create_index("idx_bf_candles_incomplete", "bf_candles", ["product_code", "timeframe", "is_complete"])


def downgrade() -> None:
    op.drop_index("idx_bf_candles_incomplete", table_name="bf_candles")
    op.drop_index("idx_bf_candles_lookup", table_name="bf_candles")
    op.drop_table("bf_candles")
