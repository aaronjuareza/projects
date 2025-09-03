# etl/steps/load.py
import pandas as pd
import numpy as np
from sqlalchemy.engine import Engine

def load_dataframe(df: pd.DataFrame, tgt_engine: Engine, table: str, if_exists="replace", chunksize=100000):
    # Reemplaza ±inf por NaN (MySQL los guardará como NULL)
    df = df.replace([np.inf, -np.inf], np.nan)

    # En columnas object, asegúrate que NaN -> None para NULL correctos (por si hubiera)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].where(pd.notnull(df[col]), None)

    df.to_sql(table, tgt_engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
