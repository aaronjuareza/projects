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
import utils

# =========================
# CONFIGURACIÓN
# =========================

config = configparser.ConfigParser()
config.read("db.config")


MYSQL_USER = config["mysql"]["user"]
MYSQL_PASSWORD = config["mysql"]["password"]
MYSQL_HOST = config["mysql"]["host"]
MYSQL_PORT = int(config["mysql"]["port"])
DB_NAME = config["mysql"]["database"]
CHUNK_SIZE = 50_000      # tamaño de lote para inserciones
SAMPLE_ROWS_FOR_DTYPE = 50_000  # filas para inferir longitudes de texto

CSV_DIR = Path("./datasource")  # carpeta con los .csv

def main():
    if not CSV_DIR.exists():
        raise FileNotFoundError(f"No existe la carpeta con CSVs: {CSV_DIR.resolve()}")

    # Conexión al servidor (sin DB) para crear la BD si no existe
    server_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/?charset=utf8mb4"
    server_engine = create_engine(server_url, pool_pre_ping=True)

    utils.create_database_if_not_exists(server_engine, DB_NAME)

    # Conexión a la BD
    db_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME}?charset=utf8mb4"
    db_engine = create_engine(db_url, pool_pre_ping=True)

    # Detectar todos los CSV
    csv_files = sorted([p for p in CSV_DIR.glob("*.csv")])
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron CSVs en {CSV_DIR.resolve()}")

    print(f"Se cargarán {len(csv_files)} archivos a la BD `{DB_NAME}`...")

    for csv_path in csv_files:
        table_name = utils.slug_table_name(csv_path.name)

        if "columns_description" in table_name:
            print(f"✓ Saltando {csv_path.name} (solo diccionario de columnas).")
            continue
        
        if utils.table_has_data(db_engine, table_name):
            print(f"✓ Saltando {table_name}, ya tiene datos.")
            continue

        print(f"\n==> Procesando: {csv_path.name} -> tabla `{table_name}`")
        utils.load_csv_to_mysql_table(db_engine, csv_path, table_name)

    print("\n✓ Carga completada.")

if __name__ == "__main__":
    main()
