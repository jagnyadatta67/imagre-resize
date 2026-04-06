"""
manage_users.py — CLI for managing ui_users (login accounts for the Flask UI).

Usage
-----
  python manage_users.py create           <username> <password>
  python manage_users.py list
  python manage_users.py deactivate       <username>
  python manage_users.py activate         <username>
  python manage_users.py reset-password   <username> <new_password>

Notes
-----
- Usernames are stored lowercase.
- Passwords are hashed with werkzeug's pbkdf2:sha256 — never stored in plain text.
- 'deactivate' is a soft-delete: the user row stays in the DB so that
  image_results.uploaded_by audit history is preserved.
- Run this script from the repo root so it can find .env automatically.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()   # loads .env from cwd (repo root)

try:
    import mysql.connector
    from werkzeug.security import generate_password_hash
except ImportError as exc:
    sys.exit(f"Missing dependency: {exc}\nRun: pip install mysql-connector-python werkzeug")


# ── DB connection ─────────────────────────────────────────────

def _connect():
    return mysql.connector.connect(
        host     = os.getenv("MYSQL_HOST",     "localhost"),
        port     = int(os.getenv("MYSQL_PORT", "3306")),
        database = os.getenv("MYSQL_DB",       "image_pipeline"),
        user     = os.getenv("MYSQL_USER",     "root"),
        password = os.getenv("MYSQL_PASSWORD", ""),
        charset  = "utf8mb4",
        collation= "utf8mb4_unicode_ci",
        autocommit=False,
    )


# ── Commands ──────────────────────────────────────────────────

def cmd_create(username: str, password: str) -> None:
    username = username.strip().lower()
    if not username:
        sys.exit("Error: username cannot be empty.")
    if len(password) < 6:
        sys.exit("Error: password must be at least 6 characters.")

    pw_hash = generate_password_hash(password)
    conn = _connect()
    cur  = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO ui_users (username, password_hash) VALUES (%s, %s)",
            (username, pw_hash),
        )
        conn.commit()
        print(f"✅  User '{username}' created successfully.")
    except mysql.connector.IntegrityError:
        sys.exit(f"Error: username '{username}' already exists.")
    finally:
        cur.close()
        conn.close()


def cmd_list() -> None:
    conn = _connect()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, username, is_active, created_at FROM ui_users ORDER BY id"
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        print("No users found.")
        return

    print(f"\n{'ID':<5}  {'USERNAME':<25}  {'ACTIVE':<8}  {'CREATED'}")
    print("-" * 65)
    for r in rows:
        active  = "✅ yes" if r["is_active"] else "❌ no"
        created = r["created_at"].strftime("%Y-%m-%d %H:%M") if r["created_at"] else "—"
        print(f"{r['id']:<5}  {r['username']:<25}  {active:<8}  {created}")
    print()


def cmd_deactivate(username: str) -> None:
    _set_active(username, 0)
    print(f"✅  User '{username}' deactivated (cannot log in; audit history preserved).")


def cmd_activate(username: str) -> None:
    _set_active(username, 1)
    print(f"✅  User '{username}' activated.")


def _set_active(username: str, flag: int) -> None:
    username = username.strip().lower()
    conn = _connect()
    cur  = conn.cursor()
    try:
        cur.execute(
            "UPDATE ui_users SET is_active = %s WHERE username = %s",
            (flag, username),
        )
        if cur.rowcount == 0:
            sys.exit(f"Error: user '{username}' not found.")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def cmd_reset_password(username: str, new_password: str) -> None:
    username = username.strip().lower()
    if len(new_password) < 6:
        sys.exit("Error: password must be at least 6 characters.")

    pw_hash = generate_password_hash(new_password)
    conn = _connect()
    cur  = conn.cursor()
    try:
        cur.execute(
            "UPDATE ui_users SET password_hash = %s WHERE username = %s",
            (pw_hash, username),
        )
        if cur.rowcount == 0:
            sys.exit(f"Error: user '{username}' not found.")
        conn.commit()
        print(f"✅  Password reset for '{username}'.")
    finally:
        cur.close()
        conn.close()


# ── Entry point ───────────────────────────────────────────────

def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0].lower()

    if cmd == "create":
        if len(args) != 3:
            sys.exit("Usage: python manage_users.py create <username> <password>")
        cmd_create(args[1], args[2])

    elif cmd == "list":
        cmd_list()

    elif cmd == "deactivate":
        if len(args) != 2:
            sys.exit("Usage: python manage_users.py deactivate <username>")
        cmd_deactivate(args[1])

    elif cmd == "activate":
        if len(args) != 2:
            sys.exit("Usage: python manage_users.py activate <username>")
        cmd_activate(args[1])

    elif cmd == "reset-password":
        if len(args) != 3:
            sys.exit("Usage: python manage_users.py reset-password <username> <new_password>")
        cmd_reset_password(args[1], args[2])

    else:
        sys.exit(f"Unknown command: '{cmd}'\n{__doc__}")


if __name__ == "__main__":
    main()
