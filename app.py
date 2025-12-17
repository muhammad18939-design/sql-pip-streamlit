import streamlit as st

# ✅ Streamlit config MUST be at top
st.set_page_config(
    page_title="SQL Data Ingestion Pipeline",
    page_icon="📥",
    layout="wide"
)

# ---- Imports (after config is OK) ----
from model import SQLPipeline
from utils import (
    save_uploaded_file,
    read_file,
    clean_headers,
    detect_sql_types
)
from cleaning_report import save_cleaning_report
import os

# ---- UI ----
st.title("📥 SQL Data Ingestion Pipeline")
st.write("Upload CSV / Excel file and push data into SQL Server")

# ---- Initialize pipeline ----
pipeline = SQLPipeline()

# ---- File uploader ----
uploaded_file = st.file_uploader(
    "Drag and drop file here",
    type=["csv", "xlsx", "xls"]
)

# ---- Table name ----
table_name = st.text_input(
    "Enter Table Name For SQL Server",
    placeholder="e.g. sales_data"
)

# ---- Main logic ----
if uploaded_file is not None:
    try:
        # 1️⃣ Save uploaded file temporarily
        file_path = save_uploaded_file(uploaded_file)

        # 2️⃣ Read file
        df_raw = read_file(file_path)

        # 3️⃣ Minimal cleaning
        df_clean, report = clean_headers(df_raw)
        sql_types = detect_sql_types(df_clean)

        st.success("✅ File loaded successfully!")

        # ---- Cleaning report ----
        st.subheader("🧹 Data Cleaning Report (Headers)")
        if report:
            for r in report:
                st.write(f"- {r}")
        else:
            st.info("No header changes were necessary.")

        # ---- Preview ----
        st.subheader("📊 File Preview")
        st.dataframe(df_clean.head(20), use_container_width=True)

        # ---- Upload to SQL ----
        if st.button("🚀 Upload to SQL Server"):
            if not table_name.strip():
                st.error("❌ Table name is required")
            else:
                with st.spinner("Uploading data to SQL Server..."):
                    # Create table
                    pipeline.create_table_if_not_exists(
                        df_clean,
                        table_name,
                        sql_types
                    )

                    # Insert data
                    pipeline.insert_data(df_clean, table_name)

                st.success(
                    f"✅ Data inserted successfully into py.dbo.{table_name}"
                )

                # ---- Save JSON report ----
                json_path = save_cleaning_report(
                    file_name=uploaded_file.name,
                    header_report=report,
                    df_cleaned=df_clean,
                    table_name=table_name,
                    database_name="py"
                )

                st.info(f"📄 Cleaning report saved: {json_path}")

        # ---- Cleanup ----
        if os.path.exists(file_path):
            os.remove(file_path)

    except Exception as e:
        st.error("❌ An error occurred")
        st.exception(e)
