import httpx
from ..utils.settings import settings
from typing import NamedTuple, List, Dict, Any

MOVIE_PATH = "/movie/top_rated"

# 클라이언트 헤더
client = httpx.Client(
    timeout=15.0, 
    headers={
        "Accept": "application/json",
        "Authorization" : f"Bearer {settings.TMDB_API_TOKEN}"
        }
    )

# tmdb 호출
def get_tmdb(path: str, **params):
    url = f"{settings.TMDB_BASE}{path}"
    # print("url: ", url)
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

# 배치
def fetch_first_page(**params):
    first = get_tmdb(MOVIE_PATH, page=1, **params)
    results = list(first.get("results") or [])
    if not results :
        print("TMDB 데이터 결과 없음")
        return [], 0
    total_pages = int(first.get("total_pages") or 0)
    print(f"첫 페이지 호출, total_pages: {total_pages}")
    return results, total_pages

def fetch_page_batch(start_page: int, end_page: int, **params):
    results: List[Dict[str, Any]] = []
    for page in range(start_page, end_page + 1):
        data = get_tmdb(MOVIE_PATH, page=page, **params)
        results.extend(data.get("results") or [])
    print(f"{start_page} ~ {end_page} 페이지 호출")
    return results


def fetch_id_batch_movie(start_idx, end_idx, id_list: List[int], **params):
    # movie_keyword, keyword 테이블
    results = []
    for idx in range(start_idx, end_idx+1):
        path = f"/movie/{id_list[idx]}/keywords"
        data = get_tmdb(path, **params)
        results.append(data or [])
    print(f"{start_idx} ~ {end_idx} 인덱스 호출")
    return results

def fetch_id_batch_credit(start_idx, end_idx, id_list: List[int], **params):
    # movie_credit_actor, movie_credit_director 테이블
    results = []
    for idx in range(start_idx, end_idx+1):
        path = f"/movie/{id_list[idx]}/credits"
        data = get_tmdb(path, **params)
        results.append(data or [])
    print(f"{start_idx} ~ {end_idx} 인덱스 호출")
    return results

def fetch_id_batch_person(start_idx, end_idx, id_list: List[int], **params):
    # person 테이블
    results = []
    for idx in range(start_idx, end_idx+1):
        path = f"/person/{id_list[idx]}"
        data = get_tmdb(path, **params)
        results.append(data or [])
    print(f"{start_idx} ~ {end_idx} 인덱스 호출")
    return results