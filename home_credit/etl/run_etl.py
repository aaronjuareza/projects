# etl/run_etl.py
import time
import argparse
from datetime import datetime
from common.config import load_config
from common.logging import setup_logger
from common.io import mysql_engine, ensure_database
from common.state import read_state, write_state
from steps.extract import (
    extract_application_train, extract_previous_application, extract_installments
)
from steps.transform import build_features
from steps.load import load_dataframe

LOGGER = setup_logger("etl")

def with_retries(func, *args, retries=3, wait_secs=3, **kwargs):
    for i in range(1, retries+1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == retries:
                raise
            LOGGER.warning(f"Intento {i}/{retries} falló: {e}. Reintentando en {wait_secs}s...")
            time.sleep(wait_secs)

def main():
    parser = argparse.ArgumentParser(description="Orquestador ETL Home Credit")
    parser.add_argument("--full", action="store_true", help="Ejecución completa (ignora watermark)")
    parser.add_argument("--since", type=str, help="ISO date para incremental (ej. 2025-01-01)")
    parser.add_argument("--dry-run", action="store_true", help="No carga a destino, solo simula")
    parser.add_argument("--limit", type=int, help="Limitar filas por tabla (para pruebas)")
    args = parser.parse_args()

    cfg = load_config()

    # Motores origen/destino
    src = cfg["source"]["mysql"]
    tgt = cfg["target"]["mysql"]

    if cfg["runtime"].get("create_schema_if_missing", True):
        ensure_database(tgt["user"], tgt["password"], tgt["host"], tgt["port"], tgt["db"])

    src_engine = mysql_engine(src["user"], src["password"], src["host"], src["port"], src["db"])
    tgt_engine = mysql_engine(tgt["user"], tgt["password"], tgt["host"], tgt["port"], tgt["db"])

    # Determinar watermark
    state = read_state()
    since = None
    if not args.full:
        if args.since:
            since = args.since
        else:
            since = state.get("since")  # p.ej., "2025-07-01"
    LOGGER.info(f"Inicio ETL | full={args.full} | since={since} | dry_run={args.dry_run} | limit={args.limit}")

    t0 = time.time()

    # === Extract ===
    # Para el dataset de Kaggle, no hay una columna de fecha global común.
    # En un banco real, aquí filtrarías por fechas > watermark.
    app = with_retries(extract_application_train, src_engine, limit=args.limit)
    prev = with_retries(extract_previous_application, src_engine, limit=args.limit)
    inst = with_retries(extract_installments, src_engine, limit=args.limit)
    LOGGER.info(f"Extract listo: app={len(app):,} prev={len(prev):,} inst={len(inst):,}")

    # === Transform ===
    features = with_retries(build_features, app, prev, inst)
    LOGGER.info(f"Transform listo: features={len(features):,} filas")

    # === Load ===
    table_final = cfg["runtime"]["table_final"]
    if not args.dry_run:
        with_retries(load_dataframe, features, tgt_engine, table_final, if_exists="replace",
                     chunksize=cfg["runtime"]["chunksize"])
        LOGGER.info(f"Load listo: tabla `{table_final}` en `{tgt['db']}`")

    # Actualiza estado
    # Sugerencia: como el dataset no tiene timestamp transversal, guarda el instante de ejecución
    new_state = {
        "since": datetime.utcnow().date().isoformat(),
        "row_counts": {"application_train": len(app), "previous_application": len(prev), "installments_payments": len(inst)}
    }
    write_state(new_state)

    LOGGER.info(f"ETL OK en {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
