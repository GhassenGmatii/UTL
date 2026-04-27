import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from .features import extract_features

def train_dedup_model(df_clean):
    features_df = extract_features(df_clean)
    if len(features_df) == 0:
        return None
    X = features_df[["name_sim", "email_sim", "phone_sim", "avg_sim"]].values
    y = (features_df["avg_sim"] > 0.7).astype(int).values
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    return model

def load_dedup_model(path):
    try:
        return joblib.load(path)
    except Exception:
        return None

def predict_duplicate_pairs(df, model, threshold=0.75):
    features_df = extract_features(df)
    if len(features_df) == 0:
        return []
    X = features_df[["name_sim", "email_sim", "phone_sim", "avg_sim"]].values
    proba = model.predict_proba(X)[:, 1]
    dup_pairs = features_df[proba >= threshold][["idx_i", "idx_j"]].values.tolist()
    return dup_pairs

def drop_predicted_duplicates(df, dup_pairs):
    indices_to_drop = set()
    for idx_i, idx_j in dup_pairs:
        indices_to_drop.add(idx_j)
    dropped = list(indices_to_drop)
    df_deduped = df.drop(index=dropped).reset_index(drop=True)
    return df_deduped, dropped

def save_dedup_model(model, path):
    joblib.dump(model, path)
