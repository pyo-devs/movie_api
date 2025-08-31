from typing import List, Dict, Any
from sqlalchemy.dialects.postgresql import insert as insert

from executor import movie_tbl, movie_info_tbl, movie_genre_tbl


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