import pyodbc
import pandas as pd
from config import SQL_SERVER, SQL_DATABASE

class SQLPipeline:

    def __init__(self):
        """
        Initialize connection to SQL Server using Windows Authentication
        """
        self.conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            "Trusted_Connection=yes;"
        )

    def create_table_if_not_exists(self, df, table_name, sql_types=None):
        """
        Create table if it does not exist.
        - df: pandas DataFrame
        - table_name: name of the table to create
        - sql_types: dict column_name -> SQL datatype (optional)
        """
        cursor = self.conn.cursor()

        if sql_types is None:
            # Safe default: NVARCHAR(MAX) for all columns
            columns = ", ".join([f"[{c}] NVARCHAR(MAX)" for c in df.columns])
        else:
            columns = ", ".join([f"[{c}] {sql_types[c]}" for c in df.columns])

        query = f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table_name}'
        )
        CREATE TABLE [{table_name}] ({columns})
        """
        cursor.execute(query)
        self.conn.commit()

    def insert_data(self, df, table_name):
        """
        Insert data safely into SQL Server:
        - NaN / pandas NA / string 'nan' -> SQL NULL
        - Everything else cast to string (safe for NVARCHAR(MAX))
        """
        cursor = self.conn.cursor()
        cols = ", ".join([f"[{c}]" for c in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO [{table_name}] ({cols}) VALUES ({placeholders})"

        for _, row in df.iterrows():
            safe_row = []
            for val in row:
                # Convert NaN / None / 'nan' to SQL NULL
                if pd.isna(val) or (isinstance(val, str) and val.lower() == "nan"):
                    safe_row.append(None)
                else:
                    safe_row.append(str(val))
            cursor.execute(query, tuple(safe_row))

        self.conn.commit()
