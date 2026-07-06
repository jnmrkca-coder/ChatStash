#!/usr/bin/env python3
"""ChatStash: self-hosted ChatGPT export organizer."""

from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import io
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import tempfile
import threading
import time
import urllib.parse
import zipfile
from datetime import datetime, timezone
from http import cookies
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any


APP_NAME = "ChatStash"
ROOT = Path(__file__).resolve().parent
INSTANCE_DIR = ROOT / "instance"
STATIC_DIR = ROOT / "static"
DB_PATH = INSTANCE_DIR / "chatstash.sqlite3"
CONFIG_PATH = INSTANCE_DIR / "config.json"
EXPORTS_DIR = INSTANCE_DIR / "exports"
DEFAULT_PORT = 8765

WRITE_LOCK = threading.RLock()
JOB_THREADS: dict[str, threading.Thread] = {}
WATCHER_THREAD: threading.Thread | None = None
WATCHER_STOP = threading.Event()

URL_RE = re.compile(r"https?://[^\s<>)\"']+", re.I)
CODE_RE = re.compile(r"```")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def from_ts(value: Any) -> str | None:
    try:
        if value is None:
            return None
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def ensure_dirs() -> None:
    INSTANCE_DIR.mkdir(exist_ok=True)
    STATIC_DIR.mkdir(exist_ok=True)
    EXPORTS_DIR.mkdir(exist_ok=True)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    ensure_dirs()
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY,
              username TEXT UNIQUE NOT NULL,
              display_name TEXT,
              password_salt TEXT NOT NULL,
              password_hash TEXT NOT NULL,
              iterations INTEGER NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
              token_hash TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              created_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS import_batches (
              id TEXT PRIMARY KEY,
              source_path TEXT NOT NULL,
              source_type TEXT NOT NULL,
              started_at TEXT NOT NULL,
              finished_at TEXT,
              status TEXT NOT NULL,
              conversation_count INTEGER DEFAULT 0,
              message TEXT
            );

            CREATE TABLE IF NOT EXISTS jobs (
              id TEXT PRIMARY KEY,
              kind TEXT NOT NULL,
              status TEXT NOT NULL,
              total INTEGER DEFAULT 0,
              done INTEGER DEFAULT 0,
              message TEXT,
              error TEXT,
              started_at TEXT NOT NULL,
              finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS source_state (
              source_path TEXT PRIMARY KEY,
              source_type TEXT NOT NULL,
              signature TEXT NOT NULL,
              last_imported_at TEXT NOT NULL,
              conversation_count INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS conversations (
              rowid INTEGER PRIMARY KEY,
              id TEXT UNIQUE NOT NULL,
              title TEXT,
              custom_title TEXT,
              created_ts REAL,
              updated_ts REAL,
              created_at TEXT,
              updated_at TEXT,
              model TEXT,
              mode TEXT,
              source TEXT DEFAULT 'ChatGPT',
              source_type TEXT,
              source_project_id TEXT,
              source_project_scope TEXT,
              source_project_label TEXT,
              original_path TEXT,
              original_inner_path TEXT,
              import_id TEXT,
              message_count INTEGER DEFAULT 0,
              user_message_count INTEGER DEFAULT 0,
              assistant_message_count INTEGER DEFAULT 0,
              system_message_count INTEGER DEFAULT 0,
              tool_message_count INTEGER DEFAULT 0,
              code_block_count INTEGER DEFAULT 0,
              url_count INTEGER DEFAULT 0,
              attachment_count INTEGER DEFAULT 0,
              has_code INTEGER DEFAULT 0,
              has_attachments INTEGER DEFAULT 0,
              is_archived INTEGER DEFAULT 0,
              is_starred INTEGER DEFAULT 0,
              is_read_only INTEGER DEFAULT 0,
              is_study_mode INTEGER DEFAULT 0,
              rating INTEGER DEFAULT 0,
              project TEXT,
              tags_json TEXT DEFAULT '[]',
              tags_flat TEXT DEFAULT '',
              custom_fields_json TEXT DEFAULT '{}',
              raw_json TEXT NOT NULL,
              text_content TEXT NOT NULL,
              metadata_json TEXT DEFAULT '{}',
              last_indexed_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_conversations_created ON conversations(created_ts);
            CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations(updated_ts);
            CREATE INDEX IF NOT EXISTS idx_conversations_model ON conversations(model);
            CREATE INDEX IF NOT EXISTS idx_conversations_project ON conversations(project);
            CREATE INDEX IF NOT EXISTS idx_conversations_import ON conversations(import_id);
            """
        )
        ensure_column(conn, "conversations", "source_project_id", "TEXT")
        ensure_column(conn, "conversations", "source_project_scope", "TEXT")
        ensure_column(conn, "conversations", "source_project_label", "TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_conversations_source_project ON conversations(source_project_id)")
        existing_fts = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='conversations_fts'"
        ).fetchone()
        if existing_fts and "content='conversations'" in (existing_fts["sql"] or ""):
            conn.executescript(
                """
                DROP TABLE conversations_fts;
                DROP TABLE IF EXISTS conversations_fts_data;
                DROP TABLE IF EXISTS conversations_fts_idx;
                DROP TABLE IF EXISTS conversations_fts_docsize;
                DROP TABLE IF EXISTS conversations_fts_config;
                """
            )
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS conversations_fts USING fts5(
              title,
              text_content,
              tags,
              project,
              model,
              metadata
            )
            """
        )
        refresh_source_project_labels(conn)


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def source_project_from_raw(raw_json: str) -> tuple[str | None, str | None]:
    try:
        conv = json.loads(raw_json)
    except json.JSONDecodeError:
        return None, None
    memory_scope = str(conv.get("memory_scope") or "")
    template_id = conv.get("conversation_template_id")
    if template_id and "project" in memory_scope:
        return str(template_id), memory_scope
    return None, None


def refresh_source_project_labels(conn: sqlite3.Connection | None = None) -> int:
    aliases = project_aliases()
    close_conn = False
    if conn is None:
        conn = db()
        close_conn = True
    updated = 0
    try:
        rows = conn.execute("SELECT rowid, raw_json, source_project_id, source_project_scope, source_project_label FROM conversations").fetchall()
        for row in rows:
            source_project_id, source_project_scope = source_project_from_raw(row["raw_json"])
            source_project_label = project_label(source_project_id, aliases)
            if (
                row["source_project_id"] == source_project_id
                and row["source_project_scope"] == source_project_scope
                and row["source_project_label"] == source_project_label
            ):
                continue
            conn.execute(
                "UPDATE conversations SET source_project_id=?, source_project_scope=?, source_project_label=? WHERE rowid=?",
                (source_project_id, source_project_scope, source_project_label, row["rowid"]),
            )
            current = conn.execute("SELECT * FROM conversations WHERE rowid=?", (row["rowid"],)).fetchone()
            conn.execute("DELETE FROM conversations_fts WHERE rowid=?", (row["rowid"],))
            conn.execute(
                "INSERT INTO conversations_fts(rowid,title,text_content,tags,project,model,metadata) VALUES (?,?,?,?,?,?,?)",
                (
                    row["rowid"],
                    current["custom_title"] or current["title"] or "",
                    current["text_content"] or "",
                    " ".join(json.loads(current["tags_json"] or "[]")),
                    current["project"] or current["source_project_label"] or "",
                    current["model"] or "",
                    current["metadata_json"] or "",
                ),
            )
            updated += 1
        return updated
    finally:
        if close_conn:
            conn.commit()
            conn.close()


def default_config() -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": DEFAULT_PORT,
        "library_paths": [str(ROOT)],
        "watch_enabled": True,
        "watch_interval_seconds": 60,
        "copy_imports_to_library": False,
        "managed_library_path": str(INSTANCE_DIR / "library"),
        "page_size": 100,
        "project_aliases": {},
    }


def load_config() -> dict[str, Any]:
    ensure_dirs()
    if not CONFIG_PATH.exists():
        cfg = default_config()
        save_config(cfg)
        return cfg
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        cfg = default_config()
    base = default_config()
    base.update(cfg)
    return base


def save_config(cfg: dict[str, Any]) -> None:
    ensure_dirs()
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


def project_aliases() -> dict[str, str]:
    aliases = load_config().get("project_aliases") or {}
    return {str(k): str(v).strip() for k, v in aliases.items() if str(v).strip()}


def project_label(project_id: str | None, aliases: dict[str, str] | None = None) -> str | None:
    if not project_id:
        return None
    aliases = aliases if aliases is not None else project_aliases()
    if aliases.get(project_id):
        return aliases[project_id]
    short = project_id
    if len(project_id) > 18:
        short = f"{project_id[:10]}...{project_id[-6:]}"
    return f"ChatGPT Project {short}"


def hash_password(password: str, salt: str | None = None, iterations: int = 310_000) -> tuple[str, str, int]:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), iterations)
    return salt, digest.hex(), iterations


def verify_password(password: str, salt: str, expected: str, iterations: int) -> bool:
    _, digest, _ = hash_password(password, salt, iterations)
    return hmac.compare_digest(digest, expected)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def json_response(handler: BaseHTTPRequestHandler, data: Any, status: int = 200) -> None:
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
    body = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def read_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0") or "0")
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def sanitize_filename(value: str, fallback: str = "conversation") -> str:
    value = (value or "").strip()
    value = re.sub(r"[\\/:*?\"<>|]+", "-", value)
    value = re.sub(r"\s+", " ", value)
    value = value.strip(" .")
    return value[:160] or fallback


def clean_tag(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def parse_tags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = re.split(r"[,;\n]+", value)
    else:
        parts = value
    tags = []
    seen = set()
    for item in parts:
        tag = clean_tag(str(item))
        if tag and tag not in seen:
            tags.append(tag)
            seen.add(tag)
    return tags


def tags_flat(tags: list[str]) -> str:
    return "|" + "|".join(tags) + "|" if tags else ""


def extract_part_text(part: Any) -> str:
    if part is None:
        return ""
    if isinstance(part, str):
        return part
    if isinstance(part, dict):
        if "text" in part and isinstance(part["text"], str):
            return part["text"]
        if "name" in part and isinstance(part["name"], str):
            return part["name"]
        return json.dumps(part, ensure_ascii=False, sort_keys=True)
    return str(part)


def extract_message_text(message: dict[str, Any]) -> str:
    content = message.get("content") or {}
    if isinstance(content, str):
        return content
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts")
    if isinstance(parts, list):
        return "\n".join(filter(None, (extract_part_text(part) for part in parts)))
    if "text" in content:
        return extract_part_text(content.get("text"))
    return ""


def sorted_messages(conv: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = []
    for key, node in (conv.get("mapping") or {}).items():
        msg = (node or {}).get("message")
        if msg:
            nodes.append(msg)
    return sorted(nodes, key=lambda m: (m.get("create_time") is None, m.get("create_time") or 0, m.get("id") or ""))


def detect_mode(conv: dict[str, Any], messages: list[dict[str, Any]]) -> str:
    if conv.get("is_study_mode"):
        return "study"
    models = set()
    async_titles = 0
    search_groups = 0
    for msg in messages:
        meta = msg.get("metadata") or {}
        if meta.get("model_slug"):
            models.add(str(meta["model_slug"]))
        if meta.get("async_task_title") or meta.get("is_async_task_result_message"):
            async_titles += 1
        if meta.get("search_result_groups"):
            search_groups += 1
    if "research" in models or search_groups:
        return "research"
    if async_titles:
        return "agent"
    return "chat"


def flatten_conversation(conv: dict[str, Any]) -> dict[str, Any]:
    messages = sorted_messages(conv)
    lines = []
    role_counts = {"user": 0, "assistant": 0, "system": 0, "tool": 0}
    models = []
    attachments = 0
    content_types = set()
    metadata_keys = set()
    for msg in messages:
        author = msg.get("author") or {}
        role = str(author.get("role") or "unknown")
        if role in role_counts:
            role_counts[role] += 1
        text = extract_message_text(msg)
        meta = msg.get("metadata") or {}
        metadata_keys.update(meta.keys())
        model = meta.get("model_slug")
        if model:
            models.append(str(model))
        if meta.get("attachments"):
            try:
                attachments += len(meta.get("attachments") or [])
            except TypeError:
                attachments += 1
        content = msg.get("content") or {}
        if isinstance(content, dict) and content.get("content_type"):
            content_types.add(str(content.get("content_type")))
        if text:
            lines.append(f"{role}: {text}")
    text_content = "\n\n".join(lines)
    default_model = conv.get("default_model_slug")
    model = str(default_model or (models[-1] if models else "") or "")
    metadata = {
        "conversation_template_id": conv.get("conversation_template_id"),
        "memory_scope": conv.get("memory_scope"),
        "pinned_time": conv.get("pinned_time"),
        "plugin_ids": conv.get("plugin_ids") or [],
        "voice": conv.get("voice"),
        "content_types": sorted(content_types),
        "metadata_keys": sorted(metadata_keys),
    }
    memory_scope = str(conv.get("memory_scope") or "")
    template_id = conv.get("conversation_template_id")
    source_project_id = str(template_id) if template_id and "project" in memory_scope else None
    return {
        "id": str(conv.get("conversation_id") or conv.get("id") or secrets.token_hex(16)),
        "title": conv.get("title") or "Untitled",
        "created_ts": conv.get("create_time"),
        "updated_ts": conv.get("update_time"),
        "created_at": from_ts(conv.get("create_time")),
        "updated_at": from_ts(conv.get("update_time")),
        "model": model,
        "mode": detect_mode(conv, messages),
        "source_project_id": source_project_id,
        "source_project_scope": memory_scope if source_project_id else None,
        "source_project_label": project_label(source_project_id),
        "message_count": len(messages),
        "user_message_count": role_counts["user"],
        "assistant_message_count": role_counts["assistant"],
        "system_message_count": role_counts["system"],
        "tool_message_count": role_counts["tool"],
        "code_block_count": len(CODE_RE.findall(text_content)),
        "url_count": len(URL_RE.findall(text_content)),
        "attachment_count": attachments,
        "has_code": 1 if CODE_RE.search(text_content) else 0,
        "has_attachments": 1 if attachments else 0,
        "is_archived": 1 if conv.get("is_archived") else 0,
        "is_starred": 1 if conv.get("is_starred") else 0,
        "is_read_only": 1 if conv.get("is_read_only") else 0,
        "is_study_mode": 1 if conv.get("is_study_mode") else 0,
        "raw_json": json.dumps(conv, ensure_ascii=False, separators=(",", ":")),
        "text_content": text_content,
        "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
    }


def upsert_conversation(conn: sqlite3.Connection, conv: dict[str, Any], source_type: str, source_path: str, inner_path: str | None, import_id: str) -> None:
    flat = flatten_conversation(conv)
    existing = conn.execute("SELECT rowid, custom_title, rating, project, tags_json, tags_flat, custom_fields_json FROM conversations WHERE id = ?", (flat["id"],)).fetchone()
    if existing:
        rowid = existing["rowid"]
        preserved = {
            "custom_title": existing["custom_title"],
            "rating": existing["rating"],
            "project": existing["project"],
            "tags_json": existing["tags_json"],
            "tags_flat": existing["tags_flat"],
            "custom_fields_json": existing["custom_fields_json"],
        }
        conn.execute(
            """
            UPDATE conversations SET
              title=?, created_ts=?, updated_ts=?, created_at=?, updated_at=?, model=?, mode=?,
              source_project_id=?, source_project_scope=?, source_project_label=?,
              source_type=?, original_path=?, original_inner_path=?, import_id=?,
              message_count=?, user_message_count=?, assistant_message_count=?, system_message_count=?, tool_message_count=?,
              code_block_count=?, url_count=?, attachment_count=?, has_code=?, has_attachments=?,
              is_archived=?, is_starred=?, is_read_only=?, is_study_mode=?,
              raw_json=?, text_content=?, metadata_json=?, last_indexed_at=?,
              custom_title=?, rating=?, project=?, tags_json=?, tags_flat=?, custom_fields_json=?
            WHERE rowid=?
            """,
            (
                flat["title"],
                flat["created_ts"],
                flat["updated_ts"],
                flat["created_at"],
                flat["updated_at"],
                flat["model"],
                flat["mode"],
                flat["source_project_id"],
                flat["source_project_scope"],
                flat["source_project_label"],
                source_type,
                source_path,
                inner_path,
                import_id,
                flat["message_count"],
                flat["user_message_count"],
                flat["assistant_message_count"],
                flat["system_message_count"],
                flat["tool_message_count"],
                flat["code_block_count"],
                flat["url_count"],
                flat["attachment_count"],
                flat["has_code"],
                flat["has_attachments"],
                flat["is_archived"],
                flat["is_starred"],
                flat["is_read_only"],
                flat["is_study_mode"],
                flat["raw_json"],
                flat["text_content"],
                flat["metadata_json"],
                utc_now(),
                preserved["custom_title"],
                preserved["rating"],
                preserved["project"],
                preserved["tags_json"],
                preserved["tags_flat"],
                preserved["custom_fields_json"],
                rowid,
            ),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO conversations (
              id,title,created_ts,updated_ts,created_at,updated_at,model,mode,source_project_id,source_project_scope,source_project_label,source_type,original_path,original_inner_path,import_id,
              message_count,user_message_count,assistant_message_count,system_message_count,tool_message_count,
              code_block_count,url_count,attachment_count,has_code,has_attachments,is_archived,is_starred,is_read_only,is_study_mode,
              raw_json,text_content,metadata_json,last_indexed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                flat["id"],
                flat["title"],
                flat["created_ts"],
                flat["updated_ts"],
                flat["created_at"],
                flat["updated_at"],
                flat["model"],
                flat["mode"],
                flat["source_project_id"],
                flat["source_project_scope"],
                flat["source_project_label"],
                source_type,
                source_path,
                inner_path,
                import_id,
                flat["message_count"],
                flat["user_message_count"],
                flat["assistant_message_count"],
                flat["system_message_count"],
                flat["tool_message_count"],
                flat["code_block_count"],
                flat["url_count"],
                flat["attachment_count"],
                flat["has_code"],
                flat["has_attachments"],
                flat["is_archived"],
                flat["is_starred"],
                flat["is_read_only"],
                flat["is_study_mode"],
                flat["raw_json"],
                flat["text_content"],
                flat["metadata_json"],
                utc_now(),
            ),
        )
        rowid = cur.lastrowid
    conn.execute("DELETE FROM conversations_fts WHERE rowid = ?", (rowid,))
    row = conn.execute("SELECT * FROM conversations WHERE rowid = ?", (rowid,)).fetchone()
    conn.execute(
        "INSERT INTO conversations_fts(rowid,title,text_content,tags,project,model,metadata) VALUES (?,?,?,?,?,?,?)",
        (
            rowid,
            row["custom_title"] or row["title"] or "",
            row["text_content"] or "",
            " ".join(json.loads(row["tags_json"] or "[]")),
            row["project"] or row["source_project_label"] or "",
            row["model"] or "",
            row["metadata_json"] or "",
        ),
    )


def looks_like_export_dir(path: Path) -> bool:
    return path.is_dir() and (path / "export_manifest.json").exists() and any(path.glob("conversations*.json"))


def find_export_dirs(path: Path) -> list[Path]:
    if not path.exists():
        return []
    if looks_like_export_dir(path):
        return [path]
    results = []
    if path.is_dir():
        for child in path.iterdir():
            if child.is_dir() and looks_like_export_dir(child):
                results.append(child)
    return results


def zip_is_export(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            has_manifest = any(name.endswith("export_manifest.json") for name in names)
            has_conversations = any(Path(name).name.startswith("conversations") and name.endswith(".json") for name in names)
            return has_manifest and has_conversations
    except zipfile.BadZipFile:
        return False


def source_signature(path: Path, kind: str) -> str:
    if kind == "zip":
        stat = path.stat()
        return f"{stat.st_size}:{stat.st_mtime_ns}"
    pieces = []
    for conv_file in sorted(path.glob("conversations*.json")):
        stat = conv_file.stat()
        pieces.append(f"{conv_file.name}:{stat.st_size}:{stat.st_mtime_ns}")
    manifest = path / "export_manifest.json"
    if manifest.exists():
        stat = manifest.stat()
        pieces.append(f"export_manifest.json:{stat.st_size}:{stat.st_mtime_ns}")
    return "|".join(pieces)


def source_changed(path: Path, kind: str) -> bool:
    resolved = str(path.resolve())
    signature = source_signature(path, kind)
    with db() as conn:
        row = conn.execute("SELECT signature FROM source_state WHERE source_path=?", (resolved,)).fetchone()
    return not row or row["signature"] != signature


def record_source_state(path: Path, kind: str, count: int) -> None:
    resolved = str(path.resolve())
    signature = source_signature(path, kind)
    with WRITE_LOCK, db() as conn:
        conn.execute(
            """
            INSERT INTO source_state(source_path,source_type,signature,last_imported_at,conversation_count)
            VALUES (?,?,?,?,?)
            ON CONFLICT(source_path) DO UPDATE SET
              source_type=excluded.source_type,
              signature=excluded.signature,
              last_imported_at=excluded.last_imported_at,
              conversation_count=excluded.conversation_count
            """,
            (resolved, kind, signature, utc_now(), count),
        )


def import_directory(path: Path, job_id: str | None = None) -> int:
    source = str(path.resolve())
    import_id = secrets.token_hex(10)
    count = 0
    files = sorted(path.glob("conversations*.json"))
    with WRITE_LOCK, db() as conn:
        conn.execute(
            "INSERT INTO import_batches(id,source_path,source_type,started_at,status,message) VALUES (?,?,?,?,?,?)",
            (import_id, source, "directory", utc_now(), "running", f"Importing {path.name}"),
        )
    for idx, conv_file in enumerate(files, start=1):
        update_job(job_id, total=len(files), done=idx - 1, message=f"Reading {conv_file.name}")
        data = json.loads(conv_file.read_text(encoding="utf-8"))
        with WRITE_LOCK, db() as conn:
            for conv in data:
                upsert_conversation(conn, conv, "directory", source, conv_file.name, import_id)
                count += 1
            conn.execute(
                "UPDATE import_batches SET conversation_count=? WHERE id=?",
                (count, import_id),
            )
        update_job(job_id, total=len(files), done=idx, message=f"Imported {count} conversations")
    with WRITE_LOCK, db() as conn:
        conn.execute(
            "UPDATE import_batches SET finished_at=?, status=?, conversation_count=?, message=? WHERE id=?",
            (utc_now(), "complete", count, f"Imported {count} conversations", import_id),
        )
    record_source_state(path, "directory", count)
    return count


def import_zip(path: Path, job_id: str | None = None) -> int:
    source = str(path.resolve())
    import_id = secrets.token_hex(10)
    count = 0
    with zipfile.ZipFile(path) as zf:
        conv_names = sorted(name for name in zf.namelist() if Path(name).name.startswith("conversations") and name.endswith(".json"))
        with WRITE_LOCK, db() as conn:
            conn.execute(
                "INSERT INTO import_batches(id,source_path,source_type,started_at,status,message) VALUES (?,?,?,?,?,?)",
                (import_id, source, "zip", utc_now(), "running", f"Importing {path.name}"),
            )
        for idx, name in enumerate(conv_names, start=1):
            update_job(job_id, total=len(conv_names), done=idx - 1, message=f"Reading {Path(name).name}")
            with zf.open(name) as fh:
                data = json.loads(fh.read().decode("utf-8"))
            with WRITE_LOCK, db() as conn:
                for conv in data:
                    upsert_conversation(conn, conv, "zip", source, name, import_id)
                    count += 1
                conn.execute("UPDATE import_batches SET conversation_count=? WHERE id=?", (count, import_id))
            update_job(job_id, total=len(conv_names), done=idx, message=f"Imported {count} conversations")
    with WRITE_LOCK, db() as conn:
        conn.execute(
            "UPDATE import_batches SET finished_at=?, status=?, conversation_count=?, message=? WHERE id=?",
            (utc_now(), "complete", count, f"Imported {count} conversations", import_id),
        )
    record_source_state(path, "zip", count)
    return count


def scan_paths(paths: list[str], job_id: str | None = None) -> int:
    total = 0
    candidates: list[tuple[str, Path]] = []
    for raw in paths:
        path = Path(raw).expanduser()
        if path.is_file() and path.suffix.lower() == ".zip" and zip_is_export(path):
            candidates.append(("zip", path))
        elif path.is_dir():
            for export_dir in find_export_dirs(path):
                candidates.append(("directory", export_dir))
            for zip_path in sorted(path.glob("*.zip")):
                if zip_is_export(zip_path):
                    candidates.append(("zip", zip_path))
    seen = set()
    unique = []
    for kind, path in candidates:
        key = (kind, str(path.resolve()))
        if key not in seen:
            unique.append((kind, path))
            seen.add(key)
    update_job(job_id, total=len(unique), done=0, message=f"Found {len(unique)} export source(s)")
    for idx, (kind, path) in enumerate(unique, start=1):
        update_job(job_id, total=len(unique), done=idx - 1, message=f"Importing {path.name}")
        if not source_changed(path, kind):
            update_job(job_id, total=len(unique), done=idx, message=f"Skipped unchanged {path.name}")
            continue
        if kind == "zip":
            total += import_zip(path, job_id)
        else:
            total += import_directory(path, job_id)
        update_job(job_id, total=len(unique), done=idx, message=f"Imported {total} conversations")
    return total


def create_job(kind: str, message: str) -> str:
    job_id = secrets.token_hex(8)
    with WRITE_LOCK, db() as conn:
        conn.execute(
            "INSERT INTO jobs(id,kind,status,total,done,message,started_at) VALUES (?,?,?,?,?,?,?)",
            (job_id, kind, "running", 0, 0, message, utc_now()),
        )
    return job_id


def update_job(job_id: str | None, **kwargs: Any) -> None:
    if not job_id:
        return
    allowed = {"status", "total", "done", "message", "error", "finished_at"}
    cols = []
    vals = []
    for key, value in kwargs.items():
        if key in allowed:
            cols.append(f"{key}=?")
            vals.append(value)
    if not cols:
        return
    vals.append(job_id)
    with WRITE_LOCK, db() as conn:
        conn.execute(f"UPDATE jobs SET {', '.join(cols)} WHERE id=?", vals)


def start_scan_job(paths: list[str]) -> str:
    job_id = create_job("scan", "Queued scan")

    def runner() -> None:
        try:
            count = scan_paths(paths, job_id)
            update_job(job_id, status="complete", finished_at=utc_now(), message=f"Imported {count} conversations")
        except Exception as exc:  # noqa: BLE001
            update_job(job_id, status="failed", finished_at=utc_now(), error=str(exc), message="Scan failed")

    thread = threading.Thread(target=runner, name=f"scan-{job_id}", daemon=True)
    JOB_THREADS[job_id] = thread
    thread.start()
    return job_id


def watcher_loop() -> None:
    while not WATCHER_STOP.wait(5):
        cfg = load_config()
        if not cfg.get("watch_enabled"):
            continue
        interval = max(15, int(cfg.get("watch_interval_seconds") or 60))
        if WATCHER_STOP.wait(interval):
            break
        paths = [str(Path(p).expanduser()) for p in cfg.get("library_paths", [])]
        if paths:
            job_id = create_job("watch-scan", "Automatic watch scan")
            try:
                count = scan_paths(paths, job_id)
                update_job(job_id, status="complete", finished_at=utc_now(), message=f"Watch imported {count} conversations")
            except Exception as exc:  # noqa: BLE001
                update_job(job_id, status="failed", finished_at=utc_now(), error=str(exc), message="Watch scan failed")


def ensure_watcher() -> None:
    global WATCHER_THREAD
    if WATCHER_THREAD and WATCHER_THREAD.is_alive():
        return
    WATCHER_THREAD = threading.Thread(target=watcher_loop, name="chatstash-watcher", daemon=True)
    WATCHER_THREAD.start()


def fts_query(user_query: str) -> str | None:
    user_query = (user_query or "").strip()
    if not user_query:
        return None
    if user_query.startswith("raw:"):
        return user_query[4:].strip()
    tokens = re.findall(r'"[^"]+"|\bAND\b|\bOR\b|\bNOT\b|[-+]?[^\s"]+', user_query, flags=re.I)
    result = []
    for token in tokens:
        upper = token.upper()
        if upper in {"AND", "OR", "NOT"}:
            result.append(upper)
            continue
        prefix = ""
        if token.startswith("-"):
            prefix = "NOT "
            token = token[1:]
        elif token.startswith("+"):
            token = token[1:]
        if token.startswith('"') and token.endswith('"'):
            term = token[1:-1].replace('"', '""')
        else:
            term = re.sub(r"[^\w.-]+", " ", token).strip()
        if term:
            result.append(prefix + '"' + term.replace('"', '""') + '"')
    return " ".join(result) or None


def build_where(params: dict[str, list[str]], args: list[Any]) -> tuple[str, str | None]:
    where = ["1=1"]
    q = (params.get("q") or [""])[0].strip()
    match = fts_query(q)
    if match:
        where.append("conversations.rowid IN (SELECT rowid FROM conversations_fts WHERE conversations_fts MATCH ?)")
        args.append(match)
    filters = {
        "model": "model",
        "mode": "mode",
    }
    for key, col in filters.items():
        val = (params.get(key) or [""])[0].strip()
        if val:
            where.append(f"{col} = ?")
            args.append(val)
    project = (params.get("project") or [""])[0].strip()
    if project:
        project_ids = [pid for pid, alias in project_aliases().items() if alias.lower() == project.lower()]
        where.append(
            "("
            "project = ? OR source_project_id = ? OR source_project_label = ?"
            + (" OR source_project_id IN (" + ",".join("?" for _ in project_ids) + ")" if project_ids else "")
            + ")"
        )
        args.extend([project, project, project])
        args.extend(project_ids)
    tag = (params.get("tag") or [""])[0].strip().lower()
    if tag:
        where.append("tags_flat LIKE ?")
        args.append(f"%|{tag}|%")
    for key, col in [("starred", "is_starred"), ("archived", "is_archived"), ("has_code", "has_code"), ("has_attachments", "has_attachments")]:
        val = (params.get(key) or [""])[0].strip()
        if val in {"0", "1"}:
            where.append(f"{col} = ?")
            args.append(int(val))
    rating = (params.get("rating") or [""])[0].strip()
    if rating:
        where.append("rating >= ?")
        args.append(int(rating))
    date_from = (params.get("date_from") or [""])[0].strip()
    if date_from:
        where.append("created_ts >= ?")
        args.append(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp())
    date_to = (params.get("date_to") or [""])[0].strip()
    if date_to:
        where.append("created_ts <= ?")
        args.append(datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp() + 86399)
    return " AND ".join(where), match


def query_conversations(params: dict[str, list[str]], include_raw: bool = False) -> dict[str, Any]:
    args: list[Any] = []
    where, match = build_where(params, args)
    sort = (params.get("sort") or ["updated_desc"])[0]
    order_map = {
        "updated_desc": "updated_ts DESC NULLS LAST",
        "updated_asc": "updated_ts ASC NULLS LAST",
        "created_desc": "created_ts DESC NULLS LAST",
        "created_asc": "created_ts ASC NULLS LAST",
        "title_asc": "COALESCE(custom_title,title) COLLATE NOCASE ASC",
        "title_desc": "COALESCE(custom_title,title) COLLATE NOCASE DESC",
        "model_asc": "model COLLATE NOCASE ASC, updated_ts DESC",
        "model_desc": "model COLLATE NOCASE DESC, updated_ts DESC",
        "mode_asc": "mode COLLATE NOCASE ASC, updated_ts DESC",
        "mode_desc": "mode COLLATE NOCASE DESC, updated_ts DESC",
        "project_asc": "COALESCE(NULLIF(project,''), source_project_label, '') COLLATE NOCASE ASC, updated_ts DESC",
        "project_desc": "COALESCE(NULLIF(project,''), source_project_label, '') COLLATE NOCASE DESC, updated_ts DESC",
        "tags_asc": "tags_flat COLLATE NOCASE ASC, updated_ts DESC",
        "tags_desc": "tags_flat COLLATE NOCASE DESC, updated_ts DESC",
        "rating_asc": "rating ASC, updated_ts DESC",
        "rating_desc": "rating DESC, updated_ts DESC",
        "stats_asc": "message_count ASC, code_block_count ASC, attachment_count ASC",
        "stats_desc": "message_count DESC, code_block_count DESC, attachment_count DESC",
        "messages_desc": "message_count DESC",
    }
    order_by = order_map.get(sort, order_map["updated_desc"])
    limit = min(500, max(1, int((params.get("limit") or ["100"])[0])))
    offset = max(0, int((params.get("offset") or ["0"])[0]))
    cols = "*" if include_raw else "rowid,id,title,custom_title,created_at,updated_at,created_ts,updated_ts,model,mode,source_type,source_project_id,source_project_scope,source_project_label,original_path,original_inner_path,import_id,message_count,user_message_count,assistant_message_count,code_block_count,url_count,attachment_count,has_code,has_attachments,is_archived,is_starred,is_read_only,is_study_mode,rating,project,tags_json,metadata_json"
    try:
        with db() as conn:
            total = conn.execute(f"SELECT COUNT(*) FROM conversations WHERE {where}", args).fetchone()[0]
            rows = conn.execute(
                f"SELECT {cols} FROM conversations WHERE {where} ORDER BY {order_by} LIMIT ? OFFSET ?",
                [*args, limit, offset],
            ).fetchall()
    except sqlite3.OperationalError:
        if not match:
            raise
        like = f"%{(params.get('q') or [''])[0].strip()}%"
        fallback_args = [like, like, *args[1:]]
        fallback_where = where.replace("conversations.rowid IN (SELECT rowid FROM conversations_fts WHERE conversations_fts MATCH ?)", "(title LIKE ? OR text_content LIKE ?)")
        with db() as conn:
            total = conn.execute(f"SELECT COUNT(*) FROM conversations WHERE {fallback_where}", fallback_args).fetchone()[0]
            rows = conn.execute(
                f"SELECT {cols} FROM conversations WHERE {fallback_where} ORDER BY {order_by} LIMIT ? OFFSET ?",
                [*fallback_args, limit, offset],
            ).fetchall()
    items = [row_to_dict(row, include_raw=include_raw) for row in rows]
    return {"items": items, "total": total, "limit": limit, "offset": offset}


def facet_values() -> dict[str, list[dict[str, Any]]]:
    with db() as conn:
        models = [
            dict(r)
            for r in conn.execute(
                """
                SELECT model value, COUNT(*) count
                FROM conversations
                WHERE model IS NOT NULL AND model != ''
                GROUP BY model
                ORDER BY count DESC, model COLLATE NOCASE
                LIMIT 300
                """
            )
        ]
        projects = [
            dict(r)
            for r in conn.execute(
                """
                SELECT COALESCE(NULLIF(project,''), source_project_label) value, COUNT(*) count
                FROM conversations
                WHERE COALESCE(NULLIF(project,''), source_project_label) IS NOT NULL
                  AND COALESCE(NULLIF(project,''), source_project_label) != ''
                GROUP BY COALESCE(NULLIF(project,''), source_project_label)
                ORDER BY count DESC, value COLLATE NOCASE
                LIMIT 300
                """
            )
        ]
        tag_counts: dict[str, int] = {}
        for row in conn.execute("SELECT tags_json FROM conversations WHERE tags_json IS NOT NULL AND tags_json != '[]'"):
            for tag in json.loads(row["tags_json"] or "[]"):
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
    tags = [
        {"value": tag, "count": count}
        for tag, count in sorted(tag_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:300]
    ]
    return {"models": models, "projects": projects, "tags": tags}


def row_to_dict(row: sqlite3.Row, include_raw: bool = False) -> dict[str, Any]:
    item = dict(row)
    item["display_title"] = item.get("custom_title") or item.get("title") or "Untitled"
    item["tags"] = json.loads(item.get("tags_json") or "[]")
    item["metadata"] = json.loads(item.get("metadata_json") or "{}")
    if item.get("source_project_id"):
        item["source_project_label"] = project_label(item.get("source_project_id"))
    item["display_project"] = item.get("project") or item.get("source_project_label") or ""
    if not include_raw:
        item.pop("tags_json", None)
        item.pop("metadata_json", None)
    return item


def markdown_for_row(row: dict[str, Any]) -> str:
    title = row.get("display_title") or row.get("custom_title") or row.get("title") or "Untitled"
    tags = row.get("tags") or json.loads(row.get("tags_json") or "[]")
    header = [
        f"# {title}",
        "",
        f"- ID: {row.get('id')}",
        f"- Created: {row.get('created_at') or ''}",
        f"- Updated: {row.get('updated_at') or ''}",
        f"- Model: {row.get('model') or ''}",
        f"- Mode: {row.get('mode') or ''}",
        f"- Project: {row.get('display_project') or row.get('project') or ''}",
        f"- Source project ID: {row.get('source_project_id') or ''}",
        f"- Rating: {row.get('rating') or 0}",
        f"- Tags: {', '.join(tags)}",
        f"- Original: {row.get('original_path') or ''} :: {row.get('original_inner_path') or ''}",
        "",
        "---",
        "",
    ]
    return "\n".join(header) + (row.get("text_content") or "") + "\n"


def filename_for_row(row: dict[str, Any], pattern: str, index: int) -> str:
    tags = row.get("tags") or json.loads(row.get("tags_json") or "[]")
    date = ""
    if row.get("created_at"):
        date = row["created_at"][:10]
    values = {
        "counter": f"{index:04d}",
        "id": row.get("id") or "",
        "date": date,
        "title": row.get("display_title") or row.get("custom_title") or row.get("title") or "Untitled",
        "project": row.get("display_project") or row.get("project") or "",
        "model": row.get("model") or "",
        "mode": row.get("mode") or "",
        "tag0": tags[0] if tags else "",
    }
    name = pattern or "{date} - {title}"
    for key, value in values.items():
        name = name.replace("{" + key + "}", str(value))
    return sanitize_filename(name, f"conversation-{index:04d}")


def rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    ids = payload.get("ids") or []
    if ids:
        placeholders = ",".join("?" for _ in ids)
        with db() as conn:
            rows = conn.execute(f"SELECT * FROM conversations WHERE id IN ({placeholders})", ids).fetchall()
        found = {row["id"]: row_to_dict(row, include_raw=True) for row in rows}
        return [found[i] for i in ids if i in found]
    params = urllib.parse.parse_qs(payload.get("query", ""), keep_blank_values=True)
    params["limit"] = [str(min(10000, int(payload.get("limit") or 10000)))]
    params["offset"] = ["0"]
    return query_conversations(params, include_raw=True)["items"]


def export_payload(payload: dict[str, Any]) -> tuple[bytes, str, str]:
    rows = rows_from_payload(payload)
    pattern = payload.get("filename_pattern") or "{date} - {title}"
    fmt = payload.get("format") or "bundle"
    if fmt == "markdown":
        content = []
        for row in rows:
            content.append(markdown_for_row(row))
            content.append("\n\n")
        return "\n".join(content).encode("utf-8"), "chatstash-export.md", "text/markdown; charset=utf-8"
    if fmt == "metadata_csv":
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(["id", "title", "created_at", "updated_at", "model", "mode", "project", "rating", "tags", "messages", "urls", "code_blocks", "attachments", "original_path", "original_inner_path"])
        for row in rows:
            writer.writerow([
                row.get("id"),
                row.get("display_title"),
                row.get("created_at"),
                row.get("updated_at"),
                row.get("model"),
                row.get("mode"),
                row.get("display_project") or row.get("project"),
                row.get("rating"),
                ", ".join(row.get("tags") or []),
                row.get("message_count"),
                row.get("url_count"),
                row.get("code_block_count"),
                row.get("attachment_count"),
                row.get("original_path"),
                row.get("original_inner_path"),
            ])
        return out.getvalue().encode("utf-8-sig"), "chatstash-metadata.csv", "text/csv; charset=utf-8"
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        manifest = {"created_at": utc_now(), "count": len(rows), "format": fmt, "app": APP_NAME}
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))
        csv_out = io.StringIO()
        writer = csv.writer(csv_out)
        writer.writerow(["id", "title", "created_at", "updated_at", "model", "mode", "project", "rating", "tags"])
        for idx, row in enumerate(rows, start=1):
            base = filename_for_row(row, pattern, idx)
            if fmt in {"zip_json", "bundle"}:
                zf.writestr(f"json/{base}.json", json.dumps(json.loads(row["raw_json"]), ensure_ascii=False, indent=2))
            if fmt in {"zip_markdown", "bundle"}:
                zf.writestr(f"markdown/{base}.md", markdown_for_row(row))
            writer.writerow([row.get("id"), row.get("display_title"), row.get("created_at"), row.get("updated_at"), row.get("model"), row.get("mode"), row.get("display_project") or row.get("project"), row.get("rating"), ", ".join(row.get("tags") or [])])
        zf.writestr("metadata.csv", csv_out.getvalue())
    return mem.getvalue(), "chatstash-bundle.zip", "application/zip"


def require_auth(handler: "ChatStashHandler") -> sqlite3.Row | None:
    raw = handler.headers.get("Cookie", "")
    jar = cookies.SimpleCookie(raw)
    morsel = jar.get("chatstash_session")
    if not morsel:
        json_response(handler, {"error": "auth_required"}, 401)
        return None
    token_hash = hash_token(morsel.value)
    with db() as conn:
        row = conn.execute(
            "SELECT users.* FROM sessions JOIN users ON users.id=sessions.user_id WHERE token_hash=?",
            (token_hash,),
        ).fetchone()
        if row:
            conn.execute("UPDATE sessions SET last_seen_at=? WHERE token_hash=?", (utc_now(), token_hash))
    if not row:
        json_response(handler, {"error": "auth_required"}, 401)
        return None
    return row


class ChatStashHandler(BaseHTTPRequestHandler):
    server_version = "ChatStash/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {self.address_string()} {fmt % args}")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/"):
            return self.handle_api_get(parsed)
        return self.serve_static(parsed.path)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/"):
            return self.handle_api_post(parsed)
        json_response(self, {"error": "not_found"}, 404)

    def do_PUT(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/"):
            return self.handle_api_post(parsed)
        json_response(self, {"error": "not_found"}, 404)

    def serve_static(self, path: str) -> None:
        if path == "/":
            path = "/index.html"
        rel = path.lstrip("/")
        target = (STATIC_DIR / rel).resolve()
        if not str(target).startswith(str(STATIC_DIR.resolve())) or not target.exists() or not target.is_file():
            text_response(self, "Not found", 404)
            return
        mime = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def handle_api_get(self, parsed: urllib.parse.ParseResult) -> None:
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if path == "/api/setup/status":
            with db() as conn:
                count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            json_response(self, {"needs_setup": count == 0, "app": APP_NAME})
            return
        user = require_auth(self)
        if not user:
            return
        if path == "/api/me":
            json_response(self, {"username": user["username"], "display_name": user["display_name"]})
        elif path == "/api/config":
            cfg = load_config()
            cfg["database_path"] = str(DB_PATH)
            json_response(self, cfg)
        elif path == "/api/stats":
            with db() as conn:
                stats = {
                    "conversation_count": conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0],
                    "message_count": conn.execute("SELECT COALESCE(SUM(message_count),0) FROM conversations").fetchone()[0],
                    "attachment_count": conn.execute("SELECT COALESCE(SUM(attachment_count),0) FROM conversations").fetchone()[0],
                    "models": [dict(r) for r in conn.execute("SELECT model, COUNT(*) count FROM conversations WHERE model != '' GROUP BY model ORDER BY count DESC LIMIT 20")],
                    "projects": [dict(r) for r in conn.execute("SELECT COALESCE(NULLIF(project,''), source_project_label) project, COUNT(*) count FROM conversations WHERE COALESCE(NULLIF(project,''), source_project_label) IS NOT NULL AND COALESCE(NULLIF(project,''), source_project_label) != '' GROUP BY COALESCE(NULLIF(project,''), source_project_label) ORDER BY count DESC LIMIT 20")],
                    "modes": [dict(r) for r in conn.execute("SELECT mode, COUNT(*) count FROM conversations GROUP BY mode ORDER BY count DESC")],
                }
            json_response(self, stats)
        elif path == "/api/facets":
            json_response(self, facet_values())
        elif path == "/api/source-projects":
            with db() as conn:
                rows = [
                    dict(r)
                    for r in conn.execute(
                        """
                        SELECT source_project_id id, source_project_scope scope, source_project_label label,
                               COUNT(*) count, MIN(created_at) first_seen, MAX(updated_at) last_seen
                        FROM conversations
                        WHERE source_project_id IS NOT NULL AND source_project_id != ''
                        GROUP BY source_project_id, source_project_scope, source_project_label
                        ORDER BY count DESC, label COLLATE NOCASE
                        """
                    )
                ]
            json_response(self, {"items": rows})
        elif path == "/api/jobs":
            with db() as conn:
                rows = [dict(r) for r in conn.execute("SELECT * FROM jobs ORDER BY started_at DESC LIMIT 30")]
            json_response(self, {"items": rows})
        elif path == "/api/imports":
            with db() as conn:
                rows = [dict(r) for r in conn.execute("SELECT * FROM import_batches ORDER BY started_at DESC LIMIT 50")]
            json_response(self, {"items": rows})
        elif path == "/api/conversations":
            json_response(self, query_conversations(params))
        elif path.startswith("/api/conversations/"):
            conv_id = urllib.parse.unquote(path.rsplit("/", 1)[-1])
            with db() as conn:
                row = conn.execute("SELECT * FROM conversations WHERE id=?", (conv_id,)).fetchone()
            if not row:
                json_response(self, {"error": "not_found"}, 404)
            else:
                item = row_to_dict(row, include_raw=True)
                item["raw"] = json.loads(item.pop("raw_json"))
                json_response(self, item)
        else:
            json_response(self, {"error": "not_found"}, 404)

    def handle_api_post(self, parsed: urllib.parse.ParseResult) -> None:
        path = parsed.path
        if path == "/api/setup":
            payload = read_body(self)
            with db() as conn:
                count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                if count:
                    json_response(self, {"error": "already_setup"}, 400)
                    return
                username = (payload.get("username") or "admin").strip()
                password = payload.get("password") or ""
                if len(password) < 10:
                    json_response(self, {"error": "password_too_short"}, 400)
                    return
                salt, digest, iterations = hash_password(password)
                conn.execute(
                    "INSERT INTO users(username,display_name,password_salt,password_hash,iterations,created_at) VALUES (?,?,?,?,?,?)",
                    (username, payload.get("display_name") or username, salt, digest, iterations, utc_now()),
                )
            json_response(self, {"ok": True})
            return
        if path == "/api/login":
            payload = read_body(self)
            username = (payload.get("username") or "").strip()
            password = payload.get("password") or ""
            with db() as conn:
                user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
                if not user or not verify_password(password, user["password_salt"], user["password_hash"], user["iterations"]):
                    json_response(self, {"error": "invalid_login"}, 401)
                    return
                token = secrets.token_urlsafe(32)
                conn.execute(
                    "INSERT INTO sessions(token_hash,user_id,created_at,last_seen_at) VALUES (?,?,?,?)",
                    (hash_token(token), user["id"], utc_now(), utc_now()),
                )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Set-Cookie", f"chatstash_session={token}; HttpOnly; SameSite=Lax; Path=/")
            body = b'{"ok":true}'
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        user = require_auth(self)
        if not user:
            return
        payload = read_body(self)
        if path == "/api/logout":
            jar = cookies.SimpleCookie(self.headers.get("Cookie", ""))
            morsel = jar.get("chatstash_session")
            if morsel:
                with db() as conn:
                    conn.execute("DELETE FROM sessions WHERE token_hash=?", (hash_token(morsel.value),))
            self.send_response(200)
            self.send_header("Set-Cookie", "chatstash_session=; Max-Age=0; HttpOnly; SameSite=Lax; Path=/")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        elif path == "/api/config":
            cfg = load_config()
            for key in ["host", "port", "library_paths", "watch_enabled", "watch_interval_seconds", "copy_imports_to_library", "managed_library_path", "page_size"]:
                if key in payload:
                    cfg[key] = payload[key]
            if "project_aliases" in payload and isinstance(payload["project_aliases"], dict):
                cfg["project_aliases"] = {
                    str(k): str(v).strip()
                    for k, v in payload["project_aliases"].items()
                    if str(k).strip() and str(v).strip()
                }
            save_config(cfg)
            with WRITE_LOCK, db() as conn:
                refresh_source_project_labels(conn)
            json_response(self, cfg)
        elif path == "/api/scan":
            paths = payload.get("paths") or load_config().get("library_paths") or [str(ROOT)]
            job_id = start_scan_job([str(p) for p in paths])
            json_response(self, {"job_id": job_id})
        elif path == "/api/conversations/bulk":
            json_response(self, bulk_update(payload))
        elif path == "/api/export":
            data, filename, content_type = export_payload(payload)
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            json_response(self, {"error": "not_found"}, 404)


def bulk_update(payload: dict[str, Any]) -> dict[str, Any]:
    ids = payload.get("ids") or []
    if not ids and payload.get("query") is not None:
        ids = [row["id"] for row in rows_from_payload(payload)]
    if not ids:
        return {"updated": 0}
    action = payload.get("action")
    with WRITE_LOCK, db() as conn:
        rows = conn.execute(f"SELECT * FROM conversations WHERE id IN ({','.join('?' for _ in ids)})", ids).fetchall()
        updated = 0
        for idx, row in enumerate(rows, start=1):
            tags = json.loads(row["tags_json"] or "[]")
            project = row["project"]
            rating = row["rating"]
            custom_title = row["custom_title"]
            archived = row["is_archived"]
            starred = row["is_starred"]
            fields = json.loads(row["custom_fields_json"] or "{}")
            if action == "add_tags":
                for tag in parse_tags(payload.get("tags")):
                    if tag not in tags:
                        tags.append(tag)
            elif action == "set_tags":
                tags = parse_tags(payload.get("tags"))
            elif action == "remove_tags":
                remove = set(parse_tags(payload.get("tags")))
                tags = [tag for tag in tags if tag not in remove]
            elif action == "set_project":
                project = (payload.get("project") or "").strip() or None
            elif action == "set_rating":
                rating = max(0, min(5, int(payload.get("rating") or 0)))
            elif action == "set_title_pattern":
                item = row_to_dict(row, include_raw=True)
                custom_title = filename_for_row(item, payload.get("pattern") or "{title}", idx)
            elif action == "set_archived":
                archived = 1 if payload.get("value") else 0
            elif action == "set_starred":
                starred = 1 if payload.get("value") else 0
            elif action == "set_custom_field":
                key = (payload.get("field") or "").strip()
                if key:
                    fields[key] = payload.get("value")
            else:
                return {"error": "unknown_action", "updated": updated}
            conn.execute(
                "UPDATE conversations SET custom_title=?, project=?, rating=?, tags_json=?, tags_flat=?, is_archived=?, is_starred=?, custom_fields_json=? WHERE rowid=?",
                (custom_title, project, rating, json.dumps(tags), tags_flat(tags), archived, starred, json.dumps(fields), row["rowid"]),
            )
            conn.execute("DELETE FROM conversations_fts WHERE rowid=?", (row["rowid"],))
            conn.execute(
                "INSERT INTO conversations_fts(rowid,title,text_content,tags,project,model,metadata) VALUES (?,?,?,?,?,?,?)",
                (row["rowid"], custom_title or row["title"] or "", row["text_content"] or "", " ".join(tags), project or "", row["model"] or "", row["metadata_json"] or ""),
            )
            updated += 1
    return {"updated": updated}


def write_static_files_if_missing() -> None:
    ensure_dirs()
    # Static files are committed with the app; this function keeps first-run resilient.
    for name in ["index.html", "app.js", "styles.css"]:
        path = STATIC_DIR / name
        if not path.exists():
            path.write_text("", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ChatStash")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--no-watch", action="store_true")
    args = parser.parse_args()
    init_db()
    write_static_files_if_missing()
    cfg = load_config()
    host = args.host or cfg.get("host") or "127.0.0.1"
    port = args.port or int(cfg.get("port") or DEFAULT_PORT)
    if not args.no_watch:
        ensure_watcher()
    server = ThreadingHTTPServer((host, port), ChatStashHandler)
    print(f"{APP_NAME} running at http://{host}:{port}")
    print(f"Database: {DB_PATH}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        WATCHER_STOP.set()
        server.server_close()


if __name__ == "__main__":
    main()
