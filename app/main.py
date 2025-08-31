import httpx
from pathlib import Path
from typing import List, Dict, Any
from datetime import date
from decimal import Decimal
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as insert

# 변수 세팅
BASE_DIR = Path(__file__).resolve().parent
class Setting(BaseSettings):
    TMDB_BASE:str
    TMDB_API_KEY:str
    TMDB_API_Token:str
    DATABASE_URL:str
    model_config = SettingsConfigDict(env_file=BASE_DIR / ".env", env_file_encoding="utf-8")

settings = Setting()

# 클라이언트 헤더
client = httpx.Client(
    timeout=15.0, 
    headers={
        "Accept": "application/json",
        "Authorization" : f"Bearer {settings.TMDB_API_Token}"
        }
    )

# db연결
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, future=True)
md = MetaData()
movie_tbl = Table("movie", md, schema="public", autoload_with=engine)
movie_info_tbl = Table("movie_info", md, schema="public", autoload_with=engine)
movie_genre_tbl = Table("movie_genre", md, schema="public", autoload_with=engine)


# tmdb 호출
def get_tmdb(path: str, **params):
    url = f"{settings.TMDB_BASE}{path}"
    print("url: ", url)
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

def set_movie() -> List[Dict[str, Any]]:
    # /movie/top_rated?language=ko-kr&page=1
    # total_pages, total_results 가져와서 모든 페이지 insert하기
    #####################################################
    data = get_tmdb("/movie/top_rated", language="ko-kr", page=1)
    print(len(data))
    if not data:
        print("TMDB 데이터 없음")
        return 0

    results = data.get("results") or []
    page = data.get("page") or ""
    total_pages = data.get("total_pages") or ""
    total_results = data.get("total_results") or ""
    print(len(results))    
    if not len(results) > 0 :
        print("TMDB 데이터 결과 없음")
        return 0
    
    # return
    # 컬럼매핑 및 테이블 설정
    movie_rows = map_movie(results)
    movie_info_rows = map_movie_info(results)
    movie_genre_rows = map_movie_genre(results)
    
    # return
    total = 0
    with engine.begin() as conn:
        if movie_rows:
            res = conn.execute(build_movie(movie_rows))
            total += res.rowcount or 0
        
        if movie_info_rows:
            res = conn.execute(build_movie_info(movie_info_rows))
            total += res.rowcount or 0

        if movie_info_rows:
            res = conn.execute(build_movie_genre(movie_genre_rows))
            total += res.rowcount or 0

        return total

def build_movie(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    ins = insert(movie_tbl).values(data)
    stmt = ins.on_conflict_do_update(
        index_elements=["movie_id"],
        set_={
            "title": ins.excluded.title,
            "original_title": ins.excluded.original_title,
            "overview": ins.excluded.overview,
            "release_date": ins.excluded.release_date,
            "adult": ins.excluded.adult,
            }
    )
    return stmt

def build_movie_info(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    ins = insert(movie_info_tbl).values(data)
    stmt = ins.on_conflict_do_update(
        index_elements=["movie_id"],
        set_={
            "popularity": ins.excluded.popularity,
            "vote_average": ins.excluded.vote_average,
            "vote_count": ins.excluded.vote_count,
            }
    )
    return stmt

def build_movie_genre(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    ins = insert(movie_genre_tbl).values(data)
    stmt = ins.on_conflict_do_nothing(
        # index_elements=["movie_id", "genre_id"]
        constraint="movie_genre_pkey"
    )
    return stmt

# 날짜변환 함수
def parse_date(d: str | None):
    if d:
        return date.fromisoformat(d)
    else:
        return None

def map_movie(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        rows.append({
            "movie_id": int(item.get("id")),
            "title": str(item.get("title") or ""),
            "original_title": str(item.get("original_title") or ""),
            "overview": item.get("overview"),
            "release_date": parse_date(item.get("release_date") or ""),
            "adult": bool(item.get("adult")),
        })
    return rows

def map_movie_info(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        rows.append({
            "movie_id": int(item.get("id")),
            "popularity": item.get("popularity"),
            "vote_average": item.get("vote_average"),
            "vote_count": item.get("vote_count"),
        })
    return rows

def map_movie_genre(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        for gid in item.get("genre_ids", []):
            rows.append({
                "movie_id": int(item.get("id")),
                "genre_id": gid,
            })
    return rows


if __name__ == "__main__":
    count = set_movie()
    print("count: ", count)