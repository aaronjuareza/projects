# etl/steps/extract.py
import pandas as pd
from sqlalchemy.engine import Engine

# Usamos application_train, previous_application, installments_payments

def extract_application_train(src_engine: Engine, limit: int | None = None) -> pd.DataFrame:
    q = "SELECT * FROM application_train"
    if limit:
        q += f" LIMIT {int(limit)}"
    return pd.read_sql(q, src_engine)

def extract_previous_application(src_engine: Engine, limit: int | None = None) -> pd.DataFrame:
    q = "SELECT * FROM previous_application"
    if limit:
        q += f" LIMIT {int(limit)}"
    return pd.read_sql(q, src_engine)

def extract_installments(src_engine: Engine, limit: int | None = None) -> pd.DataFrame:
    q = "SELECT * FROM installments_payments"
    if limit:
        q += f" LIMIT {int(limit)}"
    return pd.read_sql(q, src_engine)
