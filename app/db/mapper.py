from typing import List, Dict, Any
from itertools import islice
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

# movie_keyword 테이블
def map_movie_keyword(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        movie_id = item.get("id")
        for k in item.get("keywords", []):
            rows.append({
                "movie_id": movie_id,
                "keyword_id": int(k.get("id")),
            })
    return rows

# keyword 테이블
def map_keyword(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    #중복제거
    id_to_name = {} 
    for item in data:
        for k in item.get("keywords", []):
            try:
                kid = int(k["id"])
            except (KeyError, TypeError, ValueError):
                continue
            name = (k.get("name") or "").strip() or None
            if kid not in id_to_name:
                id_to_name[kid] = name
    rows = [{"keyword_id": kid, "keyword_name": name} for kid, name in id_to_name.items()]              
    print("키워드갯수: ", len(rows))
    return rows

# movie_credit_actor 테이블
def map_movie_credit_actor(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        movie_id = int(item.get("id"))
        for c in islice(item.get("cast", []), 10):
            rows.append({
                "movie_id": movie_id,
                "person_id": int(c.get("id")),
                "original_name": c.get("original_name"),
                "character": c.get("character"),
                "cast_order": int(c.get("order"))
            })
    return rows

# movie_credit_director 테이블
def map_movie_credit_director(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        movie_id = int(item.get("id"))
        crew_list = item.get("crew") or []
        directors = [c for c in crew_list if c.get("known_for_department") == "Directing" and c.get("department") == "Directing" and c.get("job") == "Director"]
        for d in directors:
            rows.append({
                "movie_id": movie_id,
                "person_id": int(d.get("id")),
                "original_name": d.get("original_name"),
            })
    return rows


# person 테이블
def map_person(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for item in data:
        rows.append({
            "person_id": int(item.get("id")),
            "name": item.get("name"),
            "gender": item.get("gender"),
            "person_popularity": item.get("popularity"),
            "biography": item.get("biography")
        })
    return rows