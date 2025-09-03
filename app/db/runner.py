from typing import Callable, List, Dict, Any, Tuple
from .mapper import map_movie, map_movie_info, map_movie_genre, map_movie_keyword, map_keyword, map_movie_credit_actor, map_movie_credit_director, map_person
from .builder import build_movie, build_movie_info, build_movie_genre, build_movie_keyword, build_keyword, build_movie_credit_actor, build_movie_credit_director, build_person
from .executor import execute_upserts, execute_select_movie_id, execute_select_person_id
from .tmdb_client import fetch_first_page, fetch_page_batch, fetch_id_batch_movie, fetch_id_batch_credit, fetch_id_batch_person

BATCH_SIZE: int = 5
def set_movie_first():
    # /movie/top_rated?language=ko-kr&page=1
    first_data, total_pages = fetch_first_page(language="ko-KR")
    if not first_data or not total_pages:
        print("[TMDB_movie] 첫 데이터 없음")
        return -1, 0
    res = movie_upsert(first_data)
    return res, total_pages

def set_movie_batch(start_page, end_page):
    data = fetch_page_batch(start_page, end_page, language="ko-KR")
    if not data:
        print("[TMDB_movie] 데이터 없음")
        return -1
    res = movie_upsert(data)
    return res

def movie_upsert(data: List[Dict[str, any]])-> int:
    # db컬럼매핑
    movie_rows = map_movie(data)
    movie_info_rows = map_movie_info(data)
    movie_genre_rows = map_movie_genre(data)
    # upsert결과 return
    res = execute_upserts(
        (build_movie, movie_rows),
        (build_movie_info, movie_info_rows),
        (build_movie_genre, movie_genre_rows),
    )
    return res

def run_set_movie():
    res, total_pages = set_movie_first()
    if res == -1 or not total_pages:
        print("[TMDB_movie] 첫데이터 fetch실패")
        return
    print("total_pages: ", total_pages)
    start_page = 2
    while start_page <= total_pages:
        end_page = min(start_page + BATCH_SIZE - 1, total_pages)
        res = set_movie_batch(start_page, end_page)
        print("res: ", res)
        if(not res):
            print("[movie] upsert오류")
            break
        start_page = end_page + 1

def set_movie_keyword(start_idx, end_idx, id_list) -> List[Dict[str, Any]]:
    #/movie/155/keywords
    data = fetch_id_batch_movie(start_idx, end_idx, id_list, language="ko-KR")
    if not data:
        print("[TMDB_movie_keyword] 데이터 없음")
        return -1
    
    # db컬럼매핑
    movie_keyword_rows = map_movie_keyword(data)
    keyword_rows = map_keyword(data)
    # upsert결과 return
    res = execute_upserts(
        (build_movie_keyword, movie_keyword_rows),
        (build_keyword, keyword_rows)
    )
    return res
    
def run_set_movie_keyword():
    # db에서 id리스트 추출
    id_list = execute_select_movie_id()
    if(not id_list):
        print("movie_id 추출 실패")
        return
    id_len = len(id_list)
    start_idx = 0 
    while start_idx <= id_len-1:
        end_idx = min(start_idx + BATCH_SIZE -1, id_len-1)
        res = set_movie_keyword(start_idx, end_idx, id_list)
        print("res: ", res)
        if(not res):
            print("[movie_keywore] upsert오류")
            break
        start_idx = end_idx + 1

def set_movie_credit(start_idx, end_idx, id_list) -> List[Dict[str, Any]]:
    #/movie/155/credits
    data = fetch_id_batch_credit(start_idx, end_idx, id_list, language="ko-KR")
    if not data:
        print("[TMDB_movie_credit] 데이터 없음")
        return -1
    
    # db컬럼매핑
    credit_actor_rows = map_movie_credit_actor(data)
    credit_director_rows = map_movie_credit_director(data)
    # upsert결과 return
    res = execute_upserts(
        (build_movie_credit_actor, credit_actor_rows),
        (build_movie_credit_director, credit_director_rows)
    )
    return res

def run_set_movie_credit():
    # db에서 id리스트 추출
    id_list = execute_select_movie_id()
    if(not id_list):
        print("movie_id 추출 실패")
        return
    id_len = len(id_list)
    print("id_len: ", id_len)
    start_idx = 0 
    while start_idx <= id_len-1:
        end_idx = min(start_idx + BATCH_SIZE -1, id_len-1)
        res = set_movie_credit(start_idx, end_idx, id_list)
        print("res: ", res)
        if(not res):
            print("[movie_credit] upsert오류")
            break
        start_idx = end_idx + 1

def set_person(start_idx, end_idx, id_list) -> List[Dict[str, Any]]:
    #/person/20738
    data = fetch_id_batch_person(start_idx, end_idx, id_list, language="ko-KR")
    if not data:
        print("[TMDB_person] 데이터 없음")
        return -1
    
    # db컬럼매핑
    person_rows = map_person(data)
    # upsert결과 return
    res = execute_upserts(
        (build_person, person_rows)
    )
    return res

def run_set_person():
    # db에서 id리스트 추출
    id_list = execute_select_person_id()
    if(not id_list):
        print("person_id 추출 실패")
        return
    id_len = len(id_list)
    start_idx = 0 
    while start_idx <= id_len-1:
        end_idx = min(start_idx + BATCH_SIZE -1, id_len-1)
        res = set_person(start_idx, end_idx, id_list)
        print("res: ", res)
        if(not res):
            print("[person] upsert오류")
            break
        start_idx = end_idx + 1

if __name__ == "__main__":
    # python -m app.db.runner
    run_set_movie()
    run_set_movie_keyword()
    run_set_movie_credit()
    run_set_person()
    print("완료")