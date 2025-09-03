from typing import List, Dict, Any
from sqlalchemy import Select 
from sqlalchemy.dialects.postgresql import insert as insert

from .executor import movie_tbl, movie_info_tbl, movie_genre_tbl, movie_keyword_tbl, keyword_tbl, movie_credit_actor_tbl, movie_credit_director_tbl, person_tbl

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

def build_movie_keyword(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ins = insert(movie_keyword_tbl).values(data)
    stmt = ins.on_conflict_do_nothing(
        constraint="movie_keyword_pkey"
    )
    return stmt

def build_keyword(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ins = insert(keyword_tbl).values(data)
    stmt = ins.on_conflict_do_update(
        index_elements=["keyword_id"],
        set_={
            "keyword_name": ins.excluded.keyword_name,
            }
    )
    return stmt

def build_movie_credit_actor(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ins = insert(movie_credit_actor_tbl).values(data)
    stmt = ins.on_conflict_do_update(
        index_elements=["movie_id", "person_id"],
        set_={
            "original_name": ins.excluded.original_name,
            "character": ins.excluded.character,
            "cast_order": ins.excluded.cast_order
            }
    )
    return stmt

def build_movie_credit_director(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ins = insert(movie_credit_director_tbl).values(data)
    stmt = ins.on_conflict_do_update(
        index_elements=["movie_id", "person_id"],
        set_={
            "original_name": ins.excluded.original_name,
            }
    )
    return stmt

def build_person(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ins = insert(person_tbl).values(data)
    stmt = ins.on_conflict_do_update(
        index_elements=["person_id"],
        set_={
            "name": ins.excluded.name,
            "gender": ins.excluded.gender,
            "person_popularity": ins.excluded.person_popularity,
            "biography": ins.excluded.biography,
            }
    )
    return stmt