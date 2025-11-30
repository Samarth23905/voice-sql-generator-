# python/auto_analyzer.py
import os
import sys
import json
import sqlite3
import pandas as pd
import re

def generate_sql_with_gemini(nl_query: str, schema_context: dict, gemini_key: str) -> str:
    """
    Use Google Gemini API to convert natural language query to SQL.
    Falls back to heuristic if no key provided or API call fails.
    """
    if not gemini_key:
        # Fallback heuristic
        cols = schema_context.get("columns", [])
        table = schema_context.get("table_name", "uploaded_table")
        if "marks" in [c.lower() for c in cols]:
            return f"SELECT AVG(marks) AS average_marks FROM {table};"
        return f"SELECT COUNT(*) AS row_count FROM {table};"

    try:
        import google.generativeai as genai
        genai.configure(api_key=gemini_key)
        
        # Try to use the latest available model
        try:
            model = genai.GenerativeModel("gemini-2.0-flash")
        except:
            try:
                model = genai.GenerativeModel("gemini-1.5-flash")
            except:
                model = genai.GenerativeModel("gemini-pro")
        
        # Build system context with schema info
        columns_info = ", ".join(schema_context.get("columns", []))
        table_name = schema_context.get("table_name", "users")
        
        prompt = f"""You are a SQL expert. Convert the following natural language query to a valid SQLite SQL query.

Schema:
- Table: {table_name}
- Columns: {columns_info}

Natural language query: {nl_query}

Important:
1. Return ONLY the SQL query, nothing else.
2. Use SQLite syntax.
3. Do not include explanations or markdown code blocks.
4. Ensure the query is valid and executable.

SQL:"""
        
        response = model.generate_content(prompt)
        sql_query = response.text.strip()
        
        # Clean up if Gemini wrapped in markdown code blocks
        if sql_query.startswith("```"):
            sql_query = sql_query.split("```")[1]
            if sql_query.startswith("sql"):
                sql_query = sql_query[3:]
        
        sql_query = sql_query.strip()
        
        # Ensure it ends with semicolon
        if not sql_query.endswith(";"):
            sql_query += ";"
        
        return sql_query
        
    except Exception as e:
        print(f"Gemini API error: {e}", file=sys.stderr)
        # Fall back to heuristic
        cols = schema_context.get("columns", [])
        table = schema_context.get("table_name", "uploaded_table")
        return f"SELECT COUNT(*) AS row_count FROM {table};"

def load_sql_schema_into_sqlite(conn, schema_path: str):
    with open(schema_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    # Preprocess lines to remove MySQL-specific syntax
    cleaned_lines = []
    for line in lines:
        # Skip CREATE DATABASE and USE statements
        if re.match(r"\s*(CREATE\s+DATABASE|USE)\s+", line, re.IGNORECASE):
            continue
        # Replace ENUM with TEXT
        line = re.sub(r"ENUM\s*\([^)]*\)", "TEXT", line, flags=re.IGNORECASE)
        # Replace AUTO_INCREMENT with AUTOINCREMENT first
        line = re.sub(r"AUTO_INCREMENT", "AUTOINCREMENT", line, flags=re.IGNORECASE)
        # Fix INT AUTOINCREMENT PRIMARY KEY -> INTEGER PRIMARY KEY AUTOINCREMENT
        line = re.sub(r"INT\s+AUTOINCREMENT\s+PRIMARY\s+KEY", "INTEGER PRIMARY KEY AUTOINCREMENT", line, flags=re.IGNORECASE)
        # Remove UNIQUE constraint from columns
        line = re.sub(r"\s+UNIQUE(?!\s+KEY)", "", line, flags=re.IGNORECASE)
        cleaned_lines.append(line)
    
    sql_text = "".join(cleaned_lines)
    
    # Remove ALTER TABLE ADD CONSTRAINT/KEY statements
    sql_text = re.sub(r"ALTER TABLE\s+\w+\s+ADD[^;]*;", "", sql_text, flags=re.IGNORECASE | re.MULTILINE)
    # Remove CREATE INDEX statements  
    sql_text = re.sub(r"CREATE INDEX[^;]*;", "", sql_text, flags=re.IGNORECASE | re.MULTILINE)
    
    # Remove problematic constraints from CREATE TABLE
    sql_text = re.sub(r",\s*UNIQUE\s+KEY\s+[^,)]*", "", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r",\s*KEY\s+[^,)]*", "", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r",\s*CONSTRAINT[^,)]*", "", sql_text, flags=re.IGNORECASE)
    
    # Split and execute
    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        if stmt and not stmt.startswith("--"):
            try:
                conn.execute(stmt)
                conn.commit()
            except Exception:
                pass

def infer_table_name_from_sql(sql_text: str) -> str:
    # Try to find first CREATE TABLE
    m = re.search(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)", sql_text, re.IGNORECASE)
    return m.group(1) if m else "uploaded_table"

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Missing arguments"}))
        return

    nl_query = sys.argv[1]
    schema_path = sys.argv[2]
    gemini_key = os.getenv("GEMINI_KEY", "")

    try:
        # Prepare SQLite in-memory DB
        conn = sqlite3.connect(":memory:")
        table_name = "uploaded_table"
        schema_context = {}

        if schema_path.lower().endswith(".csv"):
            df = pd.read_csv(schema_path)
            df.to_sql(table_name, conn, index=False, if_exists="replace")
            schema_context = {"columns": list(df.columns), "table_name": table_name}

        elif schema_path.lower().endswith((".sql", ".schema")):
            with open(schema_path, "r", encoding="utf-8") as f:
                sql_text = f.read()
            
            # Debug: Print first 500 chars after processing
            load_sql_schema_into_sqlite(conn, schema_path)
            
            # Verify tables were created
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cur.fetchall()
            
            if not tables:
                print(json.dumps({"error": "No tables created from schema. Schema may have unsupported syntax."}), file=sys.stderr)
            
            table_name = infer_table_name_from_sql(sql_text)
            # Attempt to fetch column info
            cols = []
            try:
                cur = conn.execute(f"PRAGMA table_info({table_name});")
                cols = [row[1] for row in cur.fetchall()]
            except Exception:
                pass
            schema_context = {"columns": cols, "table_name": table_name}

        else:
            print(json.dumps({"error": "Unsupported file type"}))
            return

        # Generate SQL via Gemini (or fallback)
        sql_query = generate_sql_with_gemini(nl_query, schema_context, gemini_key)

        # Execute generated SQL
        result_rows = []
        try:
            df_out = pd.read_sql_query(sql_query, conn)
            result_rows = df_out.to_dict(orient="records")
        except Exception as e:
            print(json.dumps({"sql": sql_query, "error": f"SQL execution failed: {str(e)}"}))
            return

        print(json.dumps({"sql": sql_query, "result": result_rows}, indent=2))

    except Exception as e:
        print(json.dumps({"error": str(e)}))

if __name__ == "__main__":
    main()