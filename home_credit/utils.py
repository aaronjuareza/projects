import os
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import (
    BIGINT, INTEGER, FLOAT, DOUBLE, DECIMAL, BOOLEAN, TEXT, VARCHAR
)
from tqdm import tqdm
import configparser

# Opcional: mapea índices por nombre de columna
INDEX_CANDIDATES = {
    # claves principales frecuentes en el dataset
    "SK_ID_CURR",
    "SK_ID_BUREAU",
    "SK_ID_PREV",
    "SK_ID_CHILD",
    "SK_ID_DEF",
    "SK_ID_PAYM"
}

CHUNK_SIZE = 50_000      # tamaño de lote para inserciones
SAMPLE_ROWS_FOR_DTYPE = 50_000  # filas para inferir longitudes de texto

# =========================
# UTILIDADES
# =========================
def slug_table_name(filename: str) -> str:
    """Convierte nombre de archivo a nombre de tabla"""
    base = Path(filename).stem.lower()
    # Ej: 'POS_CASH_balance' -> 'pos_cash_balance'
    base = re.sub(r"[^a-z0-9]+", "_", base).strip("_")
    return base

def infer_sqlalchemy_dtype(series: pd.Series, varchar_cap: int = 255):
    """Mapea dtype de pandas -> tipo SQLAlchemy razonable para MySQL."""
    if pd.api.types.is_integer_dtype(series):
        try:
            s_nonnull = series.dropna()
            if not s_nonnull.empty:
                mx = s_nonnull.max()
                mn = s_nonnull.min()
                if mx > 2_147_483_647 or mn < -2_147_483_648:
                    return BIGINT()
            return INTEGER()
        except Exception:
            return BIGINT()
    elif pd.api.types.is_float_dtype(series):
        # DOUBLE es adecuado para la mayoría de métricas continuas del dataset
        return DOUBLE()
    elif pd.api.types.is_bool_dtype(series):
        return BOOLEAN()
    else:
        # object / string
        # Estimar longitud máxima en una muestra para decidir VARCHAR vs TEXT
        try:
            s_nonnull = series.dropna().astype(str)
            if s_nonnull.empty:
                return VARCHAR(length=varchar_cap)
            sample = s_nonnull.sample(
                n=min(len(s_nonnull), SAMPLE_ROWS_FOR_DTYPE),
                random_state=42
            )
            max_len = sample.map(len).max()
            if max_len and max_len <= varchar_cap:
                # Si hay valores cortos, usar VARCHAR(255) para permitir índices
                return VARCHAR(length=varchar_cap)
            else:
                return TEXT()
        except Exception:
            return TEXT()

def build_dtype_mapping(df: pd.DataFrame):
    mapping = {}
    for col in df.columns:
        mapping[col] = infer_sqlalchemy_dtype(df[col])
    return mapping

def create_database_if_not_exists(server_engine, db_name: str):
    with server_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))

def add_useful_indexes(db_engine, table_name: str, df_columns: list[str]):
    """Crea índices sobre columnas 'clave' si existen en la tabla."""
    with db_engine.begin() as conn:
        for col in df_columns:
            if col in INDEX_CANDIDATES or col.startswith("SK_ID_"):
                try:
                    conn.execute(text(f"ALTER TABLE `{table_name}` ADD INDEX `idx_{col}` (`{col}`);"))
                except Exception as e:
                    # ya existía o no aplica
                    pass

def load_csv_to_mysql_table(db_engine, csv_path: Path, table_name: str):

    # Detectar encoding problemático
    try:
        # primer intento en utf-8
        df_head = pd.read_csv(csv_path, nrows=5, low_memory=False)
        encoding = "utf-8"
    except UnicodeDecodeError:
        # fallback común en Kaggle y Windows
        encoding = "latin1"
    # Leer CSV
    # Nota: el dataset de Home Credit tiene valores especiales (NaN, etc.). low_memory=False para dtype más estable
    # Cargar schema
    df = pd.read_csv(csv_path, nrows=1000, encoding=encoding, low_memory=False)
    dtype_map = build_dtype_mapping(df)

    # Crear tabla vacía
    df.head(0).to_sql(
        name=table_name,
        con=db_engine,
        if_exists="replace",
        index=False,
        dtype=dtype_map
    )

    # Insertar en lotes
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False), desc=f"Cargando {table_name}"):
        for col in chunk.select_dtypes(include=["object"]).columns:
            chunk[col] = chunk[col].where(pd.notnull(chunk[col]), None)

        chunk.to_sql(
            name=table_name,
            con=db_engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNK_SIZE
        )

    # Índices útiles para joins
    add_useful_indexes(db_engine, table_name, list(df.columns))

def table_has_data(db_engine, table_name: str) -> bool:
    """Devuelve True si la tabla existe y tiene filas."""
    try:
        with db_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
            count = result.scalar()
            return count > 0
    except Exception:
        # Si la tabla no existe todavía
        return False