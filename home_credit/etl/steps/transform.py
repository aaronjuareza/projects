# etl/steps/transform.py
import numpy as np
import pandas as pd

def safe_div(numer, denom):
    """Devuelve numer/denom evitando inf/NaN. Si denom<=0 o NaN -> NaN."""
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce")
    out = np.where((denom > 0) & np.isfinite(denom), numer / denom, np.nan)
    return out

def build_features(app: pd.DataFrame,
                   prev: pd.DataFrame,
                   inst: pd.DataFrame) -> pd.DataFrame:
    app = app.copy()
    prev = prev.copy()
    inst = inst.copy()

    # Relleno simple de categóricas
    for df in (app, prev, inst):
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].fillna("UNKNOWN")

    # === KPIs a nivel SK_ID_PREV desde installments ===
    if {"SK_ID_PREV", "DAYS_INSTALMENT", "DAYS_ENTRY_PAYMENT"}.issubset(inst.columns):
        inst["is_late"] = (inst["DAYS_ENTRY_PAYMENT"] > inst["DAYS_INSTALMENT"]).astype(int)
        agg_inst = inst.groupby("SK_ID_PREV").agg(
            total_installments=("is_late", "size"),
            late_installments=("is_late", "sum")
        ).reset_index()
        agg_inst["late_payment_ratio_prev"] = safe_div(agg_inst["late_installments"],
                                                       agg_inst["total_installments"])
    else:
        agg_inst = pd.DataFrame(columns=["SK_ID_PREV", "late_payment_ratio_prev"])

    # === KPIs a nivel SK_ID_CURR desde previous_application ===
    if "SK_ID_PREV" in prev.columns and "SK_ID_CURR" in prev.columns:
        prev_kpis = prev[["SK_ID_PREV","SK_ID_CURR","AMT_APPLICATION","AMT_CREDIT","NAME_CONTRACT_STATUS"]].copy()
        prev_kpis = prev_kpis.merge(agg_inst, on="SK_ID_PREV", how="left")

        # Utilización previa (evitar inf cuando AMT_APPLICATION=0/NaN)
        prev_kpis["credit_utilization_prev"] = safe_div(prev_kpis["AMT_CREDIT"], prev_kpis["AMT_APPLICATION"])

        per_client = prev_kpis.groupby("SK_ID_CURR").agg(
            prev_rejected = ("NAME_CONTRACT_STATUS", lambda s: (s == "Refused").sum()),
            prev_approved = ("NAME_CONTRACT_STATUS", lambda s: (s == "Approved").sum()),
            avg_utilization = ("credit_utilization_prev", "mean"),
            avg_late_ratio = ("late_payment_ratio_prev", "mean"),
            prev_count     = ("SK_ID_PREV", "nunique")
        ).reset_index()
    else:
        per_client = pd.DataFrame(columns=[
            "SK_ID_CURR","prev_rejected","prev_approved","avg_utilization","avg_late_ratio","prev_count"
        ])

    # === Métricas de application ===
    base_cols = ["SK_ID_CURR","AMT_INCOME_TOTAL","AMT_CREDIT","AMT_ANNUITY","TARGET"]
    base = app[[c for c in base_cols if c in app.columns]].copy()

    for c in ("AMT_INCOME_TOTAL","AMT_CREDIT","AMT_ANNUITY"):
        if c in base.columns:
            base[c] = pd.to_numeric(base[c], errors="coerce")

    # Debt-to-income con división segura (evita inf)
    base["debt_to_income_ratio"] = safe_div(base["AMT_CREDIT"], base["AMT_INCOME_TOTAL"])

    df = base.merge(per_client, on="SK_ID_CURR", how="left")

    # Rellenos y tipos
    df["avg_utilization"] = df["avg_utilization"].fillna(0.0)
    df["avg_late_ratio"]  = df["avg_late_ratio"].fillna(0.0)
    for col in ("prev_count","prev_rejected","prev_approved"):
        if col in df.columns:
            df[col] = df[col].fillna(0).astype("Int64")

    # (Opcional) Cap de ratios extremos para análisis:
    # df["debt_to_income_ratio"] = df["debt_to_income_ratio"].clip(upper=10)
    # df["avg_utilization"] = df["avg_utilization"].clip(0, 10)

    order = [
        "SK_ID_CURR","TARGET","AMT_INCOME_TOTAL","AMT_CREDIT","AMT_ANNUITY",
        "debt_to_income_ratio","prev_count","prev_approved","prev_rejected",
        "avg_utilization","avg_late_ratio"
    ]
    df = df[[c for c in order if c in df.columns]]

    # SANITIZAR: reemplazar ±inf por NaN (por si algo se coló)
    df = df.replace([np.inf, -np.inf], np.nan)

    return df
