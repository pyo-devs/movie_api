from pathlib import Path
from typing import Callable, List, Dict, Any, Tuple
from sqlalchemy import create_engine, MetaData, Table, Select
from sqlalchemy.exc import SQLAlchemyError

from ..utils import settings

# db연결
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, future=True)
md = MetaData()

# 테이블 연결
movie_tbl = Table("movie", md, schema="public", autoload_with=engine)
movie_info_tbl = Table("movie_info", md, schema="public", autoload_with=engine)
movie_genre_tbl = Table("movie_genre", md, schema="public", autoload_with=engine)
movie_credit_actor_tbl = Table("movie_credit_actor", md, schema="public", autoload_with=engine)
movie_credit_director_tbl = Table("movie_credit_director", md, schema="public", autoload_with=engine)
movie_keyword_tbl = Table("movie_keyword", md, schema="public", autoload_with=engine)
person_tbl = Table("person", md, schema="public", autoload_with=engine)
keyword_tbl = Table("keyword", md, schema="public", autoload_with=engine)

# upsert 실행
def execute_upserts (*jobs: Tuple[Callable[[List[Dict[str, Any]]], Any], List[Dict[str, Any]]]
) -> int:
    """
    jobs: (stmt_builder, rows) 튜플들의 가변인자.
          예: (build_movie, movie_rows), (build_movie_info, movie_info_rows), ...
    반환: True(정상 커밋), False(예외 발생 → 롤백)
    """
    try: 
        with engine.begin() as conn:
            for builder, rows in jobs:
                if not rows:
                    continue
                stmt = builder(rows)
                conn.execute(stmt)
        return True
    except SQLAlchemyError as e:
        print(f"movie_upsert 에러: {e}")
        return False

# movie_id 리스트 조회
def execute_select_movie_id() -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        res = conn.execute(Select(movie_tbl.c.movie_id)).scalars().all()
    return res

# person_id 리스트 조회
def execute_select_person_id() -> List[Dict[str, Any]]:
    with engine.begin() as conn:
        stmt = Select(movie_credit_actor_tbl.c.person_id).union(
            Select(movie_credit_director_tbl.c.person_id)
        )
        res = conn.execute(stmt).scalars().all()
    return res