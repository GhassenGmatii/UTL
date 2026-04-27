import pandas as pd
from .utils import normalize_text, normalize_phone, is_valid_email, parse_date_safe

def apply_cleaning_rules(df: pd.DataFrame) -> tuple:
    report = {
        "rows_before": int(len(df)),
        "exact_duplicates_removed": 0,
        "invalid_emails": 0,
        "invalid_dates": 0,
        "negative_amount_count": 0,
        "rows_after": 0,
    }

    df.columns = [c.strip().lower() for c in df.columns]

    expected_cols = ["customer_id", "name", "email", "phone", "city", "country", "signup_date", "amount"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    for col in ["name", "email", "city", "country"]:
        df[col] = df[col].apply(normalize_text)

    df["phone"] = df["phone"].apply(normalize_phone)

    invalid_email_mask = ~df["email"].apply(is_valid_email)
    report["invalid_emails"] = int(invalid_email_mask.sum())
    df.loc[invalid_email_mask, "email"] = None

    parsed_dates = df["signup_date"].apply(parse_date_safe)
    invalid_date_mask = parsed_dates.isna()
    report["invalid_dates"] = int(invalid_date_mask.sum())
    df["signup_date"] = parsed_dates

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    report["negative_amount_count"] = int((df["amount"] < 0).sum())

    content_cols = ["name", "email", "phone", "city", "country", "signup_date", "amount"]
    df = df.dropna(subset=content_cols, how="all")

    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    report["exact_duplicates_removed"] = int(before - after)

    report["rows_after"] = int(len(df))
    return df.reset_index(drop=True), report
