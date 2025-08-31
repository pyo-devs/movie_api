from typing import Callable, List, Dict, Any, Tuple

from mapper import map_movie, map_movie_info, map_movie_genre
from builder import build_movie, build_movie_info, build_movie_genre
from executor import execute_upserts
from tmdb_client import get_tmdb


def set_movie() -> List[Dict[str, Any]]:
    # /movie/top_rated?language=ko-kr&page=1
    # total_pages, total_results 가져와서 모든 페이지 insert하기
    #####################################################
    data = get_tmdb("/movie/top_rated", language="ko-kr", page=1)
    print(len(data))
    if not data:
        print("TMDB 데이터 없음")
        return

    results = data.get("results") or []
    page = data.get("page") or ""
    total_pages = data.get("total_pages") or ""
    total_results = data.get("total_results") or ""
    print(len(results))    
    if not len(results) > 0 :
        print("TMDB 데이터 결과 없음")
        return
    
    # db컬럼매핑
    movie_rows = map_movie(results)
    movie_info_rows = map_movie_info(results)
    movie_genre_rows = map_movie_genre(results)
    
    # return
    affected = execute_upserts(
        (build_movie, movie_rows),
        (build_movie_info, movie_info_rows),
        (build_movie_genre, movie_genre_rows),
    )
    return affected