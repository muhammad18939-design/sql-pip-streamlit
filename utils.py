import os
import pandas as pd
from config import ALLOWED_EXTENSIONS, TEMP_DIR

def ensure_temp_dir():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

def save_uploaded_file(uploaded_file):
    """
    Save uploaded file to TEMP_DIR
    """
    ensure_temp_dir()
    ext = uploaded_file.name.split(".")[-1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError("Unsupported file type")
    file_path = os.path.join(TEMP_DIR, uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

def read_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(file_path, dtype=str)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, dtype=str, engine='openpyxl')  # use openpyxl
    else:
        raise ValueError("Unsupported file format")


def clean_headers(df, max_len=125):
    """
    Trim column headers to max_len
    Does NOT modify data content
    Returns df and report
    """
    report = []
    new_cols = []
    for col in df.columns:
        if len(col) > max_len:
            new_col = col[:max_len]
            report.append(f"Column '{col}' trimmed to {max_len} chars -> '{new_col}'")
        else:
            new_col = col
        new_cols.append(new_col)
    df.columns = new_cols
    return df, report

def detect_sql_types(df):
    """
    Dynamically detect VARCHAR size based on data:
    - For each column, find max string length
    - Assign VARCHAR(n) based on length
    - Length > 100 -> VARCHAR(MAX)
    """
    sql_types = {}
    for col in df.columns:
        # Convert all values to string for length check, ignore NaN/None
        col_data = df[col].dropna().astype(str)
        if col_data.empty:
            sql_types[col] = "VARCHAR(MAX)"
            continue

        max_len = col_data.map(len).max()

        if max_len <= 10:
            sql_types[col] = "VARCHAR(10)"
        elif max_len <= 50:
            sql_types[col] = "VARCHAR(50)"
        elif max_len <= 100:
            sql_types[col] = "VARCHAR(100)"
        else:
            sql_types[col] = "VARCHAR(MAX)"
    return sql_types
