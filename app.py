# app.py
import os
import json
import sys
import tempfile
from flask import Flask, request, jsonify, render_template, session
import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from functools import wraps

# Add python directory to path to import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python"))
from file_extraction import extract_csv, extract_sql
from auto_analyzer import load_sql_schema_into_sqlite, generate_sql_with_gemini, infer_table_name_from_sql
import sqlite3
import pandas as pd

from dotenv import load_dotenv

# Load environment variables
load_dotenv()
PORT = int(os.getenv("PORT", "5000"))
GEMINI_KEY = os.getenv("GEMINI_KEY", "")

# Initialize a simple SQLite file DB for users
DB_PATH = os.path.join(os.path.dirname(__file__), "users.db")

def init_user_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS query_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            query TEXT NOT NULL,
            generated_sql TEXT NOT NULL,
            result_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()

init_user_db()

# Flask app setup
app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))
app.secret_key = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

# Allowed file types
ALLOWED_EXTENSIONS = {"csv", "sql", "schema"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/", methods=["GET"])
def home():
    return render_template("login.html")

@app.route("/login", methods=["GET"])
def login_form():
    return render_template("login.html")

@app.route("/login", methods=["POST"])
def login():
    data = request.get_json(force=True)
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return jsonify({"ok": False, "error": "Missing username or password"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id, password_hash FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401

        user_id, stored_hash = row
        if check_password_hash(stored_hash, password):
            session.permanent = True
            session['user_id'] = user_id
            session['username'] = username
            return jsonify({"ok": True, "redirect": "/home"})
        else:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    
@app.route("/register", methods=["GET"])
def register_form():
    return render_template("register.html")

@app.route("/register", methods=["POST"])
def register():
    data = request.get_json(force=True)
    username = data.get("username")
    password = data.get("password")
    if username and password:
        # Store user in SQLite DB with hashed password
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            password_hash = generate_password_hash(password)
            cur.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, password_hash))
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({"ok": True, "redirect": "/"})
        except sqlite3.IntegrityError:
            return jsonify({"ok": False, "error": "Username already exists"}), 400
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
    else:
        return jsonify({"ok": False, "error": "Missing username or password"}), 400


@app.route("/home", methods=["GET"])
def app_home():
    if 'user_id' not in session:
        return render_template("login.html")
    return render_template("home.html")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "port": PORT,
        "gemini_enabled": bool(GEMINI_KEY)
    })

@app.route("/api/extract-schema", methods=["POST"])
def extract_schema():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Unsupported file type. Use .csv, .sql, or .schema"}), 400

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp_file:
            file.save(tmp_file.name)
            temp_file_path = tmp_file.name
        
        # Process the file directly
        if temp_file_path.endswith(".csv"):
            result = {os.path.basename(file.filename): extract_csv(temp_file_path)}
        elif temp_file_path.endswith((".sql", ".schema")):
            result = extract_sql(temp_file_path)
        else:
            return jsonify({"error": "Unsupported file type. Use .csv, .sql, or .schema"}), 400
        
        return jsonify({"filePath": temp_file_path, "data": result})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        # Don't delete temp file yet - it will be used by generate-sql
        pass

@app.route("/api/generate-sql", methods=["POST"])
@login_required
def generate_sql():
    data = request.get_json(force=True)
    nl_query = data.get("query")
    schema_path = data.get("schemaPath")

    if not nl_query:
        return jsonify({"error": "Missing query"}), 400
    if not schema_path or not os.path.exists(schema_path):
        return jsonify({"error": "Missing or invalid schemaPath"}), 400

    try:
        # Prepare SQLite in-memory DB with schema
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
            load_sql_schema_into_sqlite(conn, schema_path)
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
            return jsonify({"error": "Unsupported file type"}), 400

        # Generate SQL via Gemini (or fallback)
        sql_query = generate_sql_with_gemini(nl_query, schema_context, GEMINI_KEY)

        # Execute generated SQL
        result_rows = []
        try:
            df_out = pd.read_sql_query(sql_query, conn)
            result_rows = df_out.to_dict(orient="records")
        except Exception as e:
            return jsonify({"sql": sql_query, "error": f"SQL execution failed: {str(e)}"}), 500

        # Save query to history
        user_id = session.get('user_id')
        try:
            db_conn = sqlite3.connect(DB_PATH)
            db_cur = db_conn.cursor()
            db_cur.execute(
                "INSERT INTO query_history (user_id, query, generated_sql, result_count) VALUES (?, ?, ?, ?)",
                (user_id, nl_query, sql_query, len(result_rows))
            )
            db_conn.commit()
            db_cur.close()
            db_conn.close()
        except Exception as e:
            print(f"Error saving query history: {e}")

        return jsonify({"sql": sql_query, "result": result_rows})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/history", methods=["GET"])
@login_required
def get_history():
    user_id = session.get('user_id')
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT query, generated_sql as sql, created_at FROM query_history WHERE user_id = ? ORDER BY created_at DESC LIMIT 50",
            (user_id,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        history = [dict(row) for row in rows]
        return jsonify({"history": history})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/session-check", methods=["GET"])
def session_check():
    if 'username' in session:
        return jsonify({"authenticated": True, "username": session.get('username')})
    return jsonify({"authenticated": False}), 401

@app.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return jsonify({"ok": True, "redirect": "/"})

if __name__ == "__main__":
    app.run(debug=True, port=PORT, host="0.0.0.0")