# etl/common/io.py
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def mysql_engine(user, password, host, port, db) -> Engine:
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True, future=True)

def ensure_database(user, password, host, port, db):
    root = f"mysql+pymysql://{user}:{password}@{host}:{port}/?charset=utf8mb4"
    eng = create_engine(root, pool_pre_ping=True, future=True)
    with eng.begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db}` "
                          "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
