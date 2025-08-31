import httpx

from utils.settings import settings

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