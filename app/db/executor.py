from pathlib import Path
from typing import Callable, List, Dict, Any, Tuple
from sqlalchemy import create_engine, MetaData, Table

from utils.settings import settings

# db연결
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, future=True)
md = MetaData()

# 테이블 연결
movie_tbl = Table("movie", md, schema="public", autoload_with=engine)
movie_info_tbl = Table("movie_info", md, schema="public", autoload_with=engine)
movie_genre_tbl = Table("movie_genre", md, schema="public", autoload_with=engine)


# 실행
def execute_upserts (*jobs: Tuple[Callable[[List[Dict[str, Any]]], Any], List[Dict[str, Any]]]
) -> int:
    """
    jobs: (stmt_builder, rows) 튜플들의 가변인자.
          예: (build_movie, movie_rows), (build_movie_info, movie_info_rows), ...
    반환: 영향을 받은 총 rowcount (None일 수 있어 or 0 처리)
    """
    total = 0
    with engine.begin() as conn:
        for builder, rows in jobs:
            if rows:
                stmt = builder(rows)
                res = conn.execute(stmt)
                total += res.rowcount or 0
    return total