import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest

def train_anomaly_model(df_clean):
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        return None
    X = df_clean[numeric_cols].fillna(df_clean[numeric_cols].mean())
    model = IsolationForest(contamination=0.1, random_state=42)
    model.fit(X)
    return model

def load_anomaly_model(path):
    try:
        return joblib.load(path)
    except Exception:
        return None

def predict_anomalies(df, model):
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        df["is_anomaly"] = 0
        df["anomaly_score"] = 0.0
        return df
    X = df[numeric_cols].fillna(df[numeric_cols].mean())
    predictions = model.predict(X)
    scores = model.score_samples(X)
    df["is_anomaly"] = (predictions == -1).astype(int)
    df["anomaly_score"] = -scores
    return df

def save_anomaly_model(model, path):
    joblib.dump(model, path)
