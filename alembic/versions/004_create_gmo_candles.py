"""gmo_candles 테이블 생성

Revision ID: 004_create_gmo_candles
Revises: 003_create_bf_funding_rates
Create Date: 2026-03-21

gmo_candles: GMO FX OHLCV 캔들 (1H / 4H)
- KLine API 폴링 수집 (tick 집계 아님)
- 복합 PK: (pair, timeframe, open_time)
- volume: GMO FX는 미제공 → 0 고정
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import Inspector

revision = "004_create_gmo_candles"
down_revision = "003_create_bf_funding_rates"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)
    if "gmo_candles" not in inspector.get_table_names():
        op.create_table(
            "gmo_candles",
            sa.Column("pair", sa.String(20), nullable=False),
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
            sa.PrimaryKeyConstraint("pair", "timeframe", "open_time", name="gmo_candles_pkey"),
        )
        op.create_index("idx_gmo_candles_lookup", "gmo_candles", ["pair", "timeframe", "open_time"])
        op.create_index("idx_gmo_candles_incomplete", "gmo_candles", ["pair", "timeframe", "is_complete"])


def downgrade() -> None:
    op.drop_index("idx_gmo_candles_incomplete", table_name="gmo_candles")
    op.drop_index("idx_gmo_candles_lookup", table_name="gmo_candles")
    op.drop_table("gmo_candles")
