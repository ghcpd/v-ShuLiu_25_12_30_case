import os
import random
import sqlite3
import tempfile

import pytest


@pytest.fixture(autouse=True)
def deterministic_seed():
    random.seed(1)


@pytest.fixture()
def tmp_db_path(tmp_path):
    p = tmp_path / "test_meta.db"
    return str(p)


@pytest.fixture()
def small_tasks_file(tmp_path):
    p = tmp_path / "tasks.json"
    data = [
        {"image_key": "img/1.jpg", "size": "200x200"},
        {"image_key": "img/2.jpg", "size": "400x400"},
        {"image_key": "img/3.jpg", "size": "800x800"},
    ]
    p.write_text(str(data).replace("'", '"'), encoding="utf-8")
    return str(p)
