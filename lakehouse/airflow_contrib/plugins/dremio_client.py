import os
from typing import List

import requests

DREMIO_API = os.getenv("DREMIO_API", "http://dremio:9047/api/v3")


def get_session() -> requests.Session:
    session = requests.Session()
    username = os.getenv("DREMIO_ADMIN_USER", "dremio")
    password = os.getenv("DREMIO_ADMIN_PASSWORD", "dremio")
    resp = session.post(
        f"{DREMIO_API}/login",
        json={"userName": username, "password": password},
        timeout=10,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    session.headers["authorization"] = f"_dremio{token}"
    return session


def refresh_reflections(datasets: List[str]):
    session = get_session()
    for dataset in datasets:
        resp = session.post(
            f"{DREMIO_API}/reflections/refresh",
            json={"path": dataset},
            timeout=10,
        )
        resp.raise_for_status()

