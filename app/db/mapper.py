from typing import List, Dict, Any
from datetime import date

# 날짜변환 함수
def parse_date(d: str | None):
    if d:
        return date.fromisoformat(d)
    else:
        return None

# movie 테이블
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

# movie_info 테이블
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

# movie_genre 테이블
def map_movie_genre(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        for gid in item.get("genre_ids", []):
            rows.append({
                "movie_id": int(item.get("id")),
                "genre_id": gid,
            })
    return rows
