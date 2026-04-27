import pandas as pd
from rapidfuzz import fuzz

def compute_name_similarity(name1, name2):
    if pd.isna(name1) or pd.isna(name2):
        return 0.0
    return fuzz.token_set_ratio(str(name1), str(name2)) / 100.0

def compute_email_similarity(email1, email2):
    if pd.isna(email1) or pd.isna(email2):
        return 0.0
    return fuzz.ratio(str(email1), str(email2)) / 100.0

def compute_phone_similarity(phone1, phone2):
    if pd.isna(phone1) or pd.isna(phone2):
        return 0.0
    return fuzz.ratio(str(phone1), str(phone2)) / 100.0

def create_pair_features(df):
    pairs = []
    for i in range(len(df)):
        for j in range(i + 1, len(df)):
            row_i = df.iloc[i]
            row_j = df.iloc[j]
            name_sim = compute_name_similarity(row_i["name"], row_j["name"])
            email_sim = compute_email_similarity(row_i["email"], row_j["email"])
            phone_sim = compute_phone_similarity(row_i["phone"], row_j["phone"])
            pairs.append({
                "idx_i": i,
                "idx_j": j,
                "name_sim": name_sim,
                "email_sim": email_sim,
                "phone_sim": phone_sim,
                "avg_sim": (name_sim + email_sim + phone_sim) / 3.0,
            })
    return pd.DataFrame(pairs) if pairs else pd.DataFrame()

def extract_features(df):
    return create_pair_features(df)
