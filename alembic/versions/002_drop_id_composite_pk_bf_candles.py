"""Remove id from bf_candles, promote composite PK

Revision ID: 002_drop_id_composite_pk_bf_candles
Revises: 001
Create Date: 2026-03-11

upsert 때마다 시퀀스가 증가해 id 값이 불필요하게 커지는 문제 해소.
기존 유일성 보장은 (product_code, timeframe, open_time) 복합 PK로 대체.

기존 데이터 : 보존 (변경 없음)
기존 인덱스 : 보존
기존 uq_ 제약 : 제거 (PK가 동일 역할 수행)
"""
from alembic import op
import sqlalchemy as sa

revision = "002_bf_candles_composite_pk"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. 기존 unique 제약 제거 (PK로 흡수)
    op.drop_constraint("uq_bf_candles_pc_tf_open", "bf_candles", type_="unique")

    # 2. 기존 PK (id) 제거
    op.drop_constraint("bf_candles_pkey", "bf_candles", type_="primary")

    # 3. 복합 PK 추가
    op.execute(
        "ALTER TABLE bf_candles ADD PRIMARY KEY (product_code, timeframe, open_time)"
    )

    # 4. id 컬럼 및 연결된 시퀀스 제거
    op.drop_column("bf_candles", "id")


def downgrade() -> None:
    # 복합 PK 제거
    op.drop_constraint("bf_candles_pkey", "bf_candles", type_="primary")

    # id 컬럼 복원 (시퀀스 포함)
    op.add_column(
        "bf_candles",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=True),
    )
    # 기존 행에 시퀀스 값 채우기
    op.execute("CREATE SEQUENCE IF NOT EXISTS bf_candles_id_seq")
    op.execute(
        "UPDATE bf_candles SET id = nextval('bf_candles_id_seq')"
    )
    op.execute(
        "ALTER TABLE bf_candles ALTER COLUMN id SET NOT NULL"
    )
    op.execute(
        "ALTER TABLE bf_candles ALTER COLUMN id SET DEFAULT nextval('bf_candles_id_seq')"
    )
    op.execute(
        "ALTER SEQUENCE bf_candles_id_seq OWNED BY bf_candles.id"
    )

    # id PK 복원
    op.create_primary_key("bf_candles_pkey", "bf_candles", ["id"])

    # unique 제약 복원
    op.create_unique_constraint(
        "uq_bf_candles_pc_tf_open", "bf_candles", ["product_code", "timeframe", "open_time"]
    )
