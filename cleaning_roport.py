import json
import os

def save_cleaning_report(
    file_name, 
    header_report, 
    df_cleaned, 
    table_name, 
    database_name, 
    output_dir="cleaning_reports"
):
    """
    Save a JSON report for each uploaded file showing data cleaning performed.

    Parameters:
    - file_name: str, original Excel/CSV file name
    - header_report: list of strings describing header trims
    - df_cleaned: pandas DataFrame with cleaned headers
    - table_name: str, SQL Server table name
    - database_name: str, SQL Server database name
    - output_dir: folder to save JSON reports (default: 'cleaning_reports')

    Returns:
    - json_file_path: path to the saved JSON file
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Prepare JSON structure
    report_data = {
        "file_name": file_name,
        "header_cleaning": [],
        "sql_table_name": table_name,
        "sql_database_name": database_name
    }

    for report_line in header_report:
        # Expected format: "Column 'ORIGINAL' trimmed to 125 chars -> 'CLEANED'"
        try:
            original = report_line.split("Column '")[1].split("' trimmed")[0]
            cleaned = report_line.split("-> '")[1][:-1]  # Remove ending quote
        except Exception:
            original = ""
            cleaned = ""
        report_data["header_cleaning"].append({
            "original_column": original,
            "cleaned_column": cleaned
        })

    # Save JSON file
    json_file_name = os.path.splitext(file_name)[0] + "_cleaning_report.json"
    json_file_path = os.path.join(output_dir, json_file_name)

    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=4)

    return json_file_path
