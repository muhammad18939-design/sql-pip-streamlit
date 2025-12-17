import streamlit as st
from model import SQLPipeline
from utils import save_uploaded_file, read_file, clean_headers, detect_sql_types
from cleaning_report import save_cleaning_report
import os

st.set_page_config(page_title="SQL Data Ingestion Pipeline")
st.title("📥 SQL Data Ingestion Pipeline")

pipeline = SQLPipeline()

uploaded_file = st.file_uploader(
    "Drag and drop file here",
    type=["csv", "xlsx", "xls"]
)

table_name = st.text_input("Enter Table Name For SQL Server")

if uploaded_file:
    file_path = save_uploaded_file(uploaded_file)
    df_raw = read_file(file_path)

    # ---- Minimal Cleaning Layer ----
    df_clean, report = clean_headers(df_raw)
    sql_types = detect_sql_types(df_clean)

    st.success("File loaded successfully!")

    if report:
        st.subheader(" Data Cleaning Report (Headers Only)")
        for r in report:
            st.write(f"- {r}")
    else:
        st.info("No header changes were necessary.")

    st.subheader("File Preview")
    st.dataframe(df_clean.head(21), use_container_width=True)

    if st.button("Upload to SQL Server"):
        if not table_name:
            st.error("Table name is required")
        else:
            # 1️⃣ Create table if not exists
            pipeline.create_table_if_not_exists(df_clean, table_name, sql_types)

            # 2️⃣ Insert data safely
            pipeline.insert_data(df_clean, table_name)
            st.success(f"Data inserted successfully into py.dbo.{table_name}")

            # 3️⃣ Save real-time JSON cleaning report
            json_path = save_cleaning_report(
                file_name=uploaded_file.name,
                header_report=report,
                df_cleaned=df_clean,
                table_name=table_name,
                database_name="py"  # Replace if dynamic DB
            )
            st.info(f"Cleaning report saved: {json_path}")

            # 4️⃣ Remove temporary uploaded file
            os.remove(file_path)
