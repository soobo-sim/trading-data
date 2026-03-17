"""bf_funding_rates 테이블 생성

Revision ID: 003_create_bf_funding_rates
Revises: 002_bf_candles_composite_pk
Create Date: 2026-03-15

bf_funding_rates: BitFlyer FX_BTC_JPY 펀딩레이트 수집 이력
- 15분 폴링으로 수집
- current_funding_rate: 현재 펀딩레이트
- next_settlement_date: 다음 정산 일시
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine import Inspector

revision = "003_create_bf_funding_rates"
down_revision = "002_bf_candles_composite_pk"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)
    if "bf_funding_rates" not in inspector.get_table_names():
        op.create_table(
            "bf_funding_rates",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("product_code", sa.String(20), nullable=False),
            sa.Column("current_funding_rate", sa.Numeric(12, 8), nullable=False),
            sa.Column("next_settlement_date", sa.DateTime(timezone=True), nullable=True),
            sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index("idx_bf_funding_rates_lookup", "bf_funding_rates", ["product_code", "collected_at"])
        op.create_unique_constraint(
            "uq_bf_funding_rates_pc_collected", "bf_funding_rates", ["product_code", "collected_at"]
        )
    else:
        # 이미 테이블 존재 — UNIQUE constraint 및 index 누락 여부만 보완
        existing_constraints = [c["name"] for c in inspector.get_unique_constraints("bf_funding_rates")]
        if "uq_bf_funding_rates_pc_collected" not in existing_constraints:
            op.create_unique_constraint(
                "uq_bf_funding_rates_pc_collected", "bf_funding_rates", ["product_code", "collected_at"]
            )
        existing_indexes = [i["name"] for i in inspector.get_indexes("bf_funding_rates")]
        if "idx_bf_funding_rates_lookup" not in existing_indexes:
            op.create_index("idx_bf_funding_rates_lookup", "bf_funding_rates", ["product_code", "collected_at"])


def downgrade() -> None:
    op.drop_index("idx_bf_funding_rates_lookup", table_name="bf_funding_rates")
    op.drop_table("bf_funding_rates")
