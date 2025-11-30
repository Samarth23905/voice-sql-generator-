import pandas as pd
import sys
import os
import json
import re

def extract_csv(file_path):
    df = pd.read_csv(file_path)
    return {
        "columns": list(df.columns),
        "rows": df.head(10).to_dict(orient="records"),
        "row_count": len(df)
    }

def extract_sql(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    tables = {}
    # Find CREATE TABLE statements
    create_table_pattern = r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)\s*\((.*?)\);"
    matches = re.findall(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    for table_name, table_def in matches:
        tables[table_name] = {
            "columns": [],
            "create_statement": f"CREATE TABLE {table_name} (...)"
        }
        # Extract column names
        columns = re.findall(r"(\w+)\s+\w+", table_def)
        tables[table_name]["columns"] = columns[:20]  # Limit to first 20 columns
    
    # Find INSERT INTO statements grouped by table
    insert_pattern = r"INSERT INTO\s+(\w+).*?;"
    insert_statements = re.findall(insert_pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    for table_name in set(insert_statements):
        if table_name not in tables:
            tables[table_name] = {"insert_count": 0, "sample_inserts": []}
        count = insert_statements.count(table_name)
        tables[table_name]["insert_count"] = count
        tables[table_name]["sample_inserts"] = f"{count} INSERT statements found"

    return tables

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No file path provided"}))
        return

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(json.dumps({"error": "File not found"}))
        return

    if file_path.endswith(".csv"):
        result = {os.path.basename(file_path): extract_csv(file_path)}
    elif file_path.endswith((".sql", ".schema")):
        result = extract_sql(file_path)
    else:
        result = {"error": "Unsupported file type. Use .csv, .sql, or .schema"}

    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()