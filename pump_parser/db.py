"""SQLite database for parsed pump results.

Tables:
    catalogs  — parsed PDF catalogs (source, timestamp, stats)
    pumps     — individual pump entries with all fields
    jobs      — parse job history

All operations are synchronous (sqlite3). Thread-safe via check_same_thread=False.
"""

import json
import sqlite3
import logging
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone

from pump_parser.config import DB_PATH
from pump_parser.models import PumpEntry, ParseResult

log = logging.getLogger("pump_parser.db")

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS catalogs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    filename    TEXT NOT NULL,
    parsed_at   TEXT NOT NULL,
    total_models    INTEGER DEFAULT 0,
    pages_processed INTEGER DEFAULT 0,
    pages_skipped   INTEGER DEFAULT 0,
    extraction_time_s REAL DEFAULT 0.0,
    avg_confidence  REAL DEFAULT 0.0,
    recipe_used     TEXT,
    quality_verdict TEXT,
    UNIQUE(filename)
);

CREATE TABLE IF NOT EXISTS pumps (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    catalog_id  INTEGER NOT NULL REFERENCES catalogs(id) ON DELETE CASCADE,
    model       TEXT NOT NULL,
    series      TEXT DEFAULT '',
    manufacturer TEXT DEFAULT '',
    article     TEXT DEFAULT '',
    q_nom       REAL DEFAULT 0.0,
    h_nom       REAL DEFAULT 0.0,
    power_kw    REAL DEFAULT 0.0,
    rpm         INTEGER DEFAULT 0,
    efficiency  REAL DEFAULT 0.0,
    q_points    TEXT DEFAULT '[]',
    h_points    TEXT DEFAULT '[]',
    dn_suction  INTEGER DEFAULT 0,
    dn_discharge INTEGER DEFAULT 0,
    weight_kg   REAL DEFAULT 0.0,
    voltage     TEXT DEFAULT '',
    phases      INTEGER DEFAULT 3,
    stages      INTEGER DEFAULT 0,
    source_file TEXT DEFAULT '',
    source_page INTEGER DEFAULT 0,
    data_source TEXT DEFAULT '',
    confidence  REAL DEFAULT 0.0,
    recipe_id   TEXT DEFAULT '',
    warnings    TEXT DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_pumps_catalog ON pumps(catalog_id);
CREATE INDEX IF NOT EXISTS idx_pumps_model ON pumps(model);
CREATE INDEX IF NOT EXISTS idx_pumps_series ON pumps(series);
CREATE INDEX IF NOT EXISTS idx_pumps_q ON pumps(q_nom);
CREATE INDEX IF NOT EXISTS idx_pumps_h ON pumps(h_nom);
CREATE INDEX IF NOT EXISTS idx_pumps_p ON pumps(power_kw);

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    started_at  TEXT,
    completed_at TEXT,
    catalog_id  INTEGER REFERENCES catalogs(id),
    error       TEXT
);
"""


class PumpDB:
    """SQLite database for pump catalog data."""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = str(db_path or DB_PATH)
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._get_conn()
        conn.executescript(_CREATE_SQL)
        conn.commit()
        log.debug("DB initialized: %s", self.db_path)

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path, check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    @contextmanager
    def _tx(self):
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ─── Catalog operations ───────────────────────────────────────────────

    def save_result(self, result: ParseResult, quality_verdict: str = "") -> int:
        """Save a ParseResult to the database. Returns catalog_id."""
        filename = Path(result.source).name
        now = datetime.now(timezone.utc).isoformat()

        avg_conf = (
            sum(e.confidence for e in result.entries) / len(result.entries)
            if result.entries else 0.0
        )

        with self._tx() as conn:
            # Upsert catalog
            conn.execute(
                """INSERT INTO catalogs
                   (source, filename, parsed_at, total_models, pages_processed,
                    pages_skipped, extraction_time_s, avg_confidence, recipe_used, quality_verdict)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(filename) DO UPDATE SET
                    parsed_at=excluded.parsed_at,
                    total_models=excluded.total_models,
                    pages_processed=excluded.pages_processed,
                    pages_skipped=excluded.pages_skipped,
                    extraction_time_s=excluded.extraction_time_s,
                    avg_confidence=excluded.avg_confidence,
                    recipe_used=excluded.recipe_used,
                    quality_verdict=excluded.quality_verdict
                """,
                (result.source, filename, now, len(result.entries),
                 result.pages_processed, result.pages_skipped,
                 round(result.extraction_time_s, 2), round(avg_conf, 3),
                 result.recipe_used, quality_verdict),
            )

            catalog_id = conn.execute(
                "SELECT id FROM catalogs WHERE filename = ?", (filename,)
            ).fetchone()["id"]

            # Delete old pumps for this catalog (re-parse replaces)
            conn.execute("DELETE FROM pumps WHERE catalog_id = ?", (catalog_id,))

            # Insert pumps
            for e in result.entries:
                conn.execute(
                    """INSERT INTO pumps
                       (catalog_id, model, series, manufacturer, article,
                        q_nom, h_nom, power_kw, rpm, efficiency,
                        q_points, h_points,
                        dn_suction, dn_discharge, weight_kg, voltage, phases, stages,
                        source_file, source_page, data_source, confidence, recipe_id, warnings)
                       VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?, ?,?,?,?,?,?, ?,?,?,?,?,?)
                    """,
                    (catalog_id, e.model, e.series, e.manufacturer, e.article,
                     e.q_nom, e.h_nom, e.power_kw, e.rpm, e.efficiency,
                     json.dumps(e.q_points), json.dumps(e.h_points),
                     e.dn_suction, e.dn_discharge, e.weight_kg, e.voltage, e.phases, e.stages,
                     e.source_file, e.source_page, e.data_source, e.confidence, e.recipe_id,
                     json.dumps(e.warnings)),
                )

        log.info("Saved %d pumps from %s (catalog_id=%d)", len(result.entries), filename, catalog_id)
        return catalog_id

    def get_catalog(self, catalog_id: int) -> dict | None:
        """Get catalog metadata by ID."""
        row = self._get_conn().execute(
            "SELECT * FROM catalogs WHERE id = ?", (catalog_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_catalogs(self) -> list[dict]:
        """List all parsed catalogs."""
        rows = self._get_conn().execute(
            "SELECT * FROM catalogs ORDER BY parsed_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_catalog(self, catalog_id: int) -> bool:
        """Delete catalog and its pumps."""
        with self._tx() as conn:
            conn.execute("DELETE FROM jobs WHERE catalog_id = ?", (catalog_id,))
            conn.execute("DELETE FROM pumps WHERE catalog_id = ?", (catalog_id,))
            cur = conn.execute("DELETE FROM catalogs WHERE id = ?", (catalog_id,))
            return cur.rowcount > 0

    # ─── Pump queries ─────────────────────────────────────────────────────

    def get_pumps(self, catalog_id: int) -> list[dict]:
        """Get all pumps for a catalog."""
        rows = self._get_conn().execute(
            "SELECT * FROM pumps WHERE catalog_id = ? ORDER BY model",
            (catalog_id,),
        ).fetchall()
        return [self._pump_row_to_dict(r) for r in rows]

    def search_pumps(
        self,
        q_min: float | None = None,
        q_max: float | None = None,
        h_min: float | None = None,
        h_max: float | None = None,
        p_min: float | None = None,
        p_max: float | None = None,
        model: str | None = None,
        series: str | None = None,
        manufacturer: str | None = None,
        min_confidence: float = 0.0,
        has_curve: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """Search pumps with filters. Returns (results, total_count)."""
        where = ["1=1"]
        params: list = []

        if q_min is not None:
            where.append("q_nom >= ?")
            params.append(q_min)
        if q_max is not None:
            where.append("q_nom <= ?")
            params.append(q_max)
        if h_min is not None:
            where.append("h_nom >= ?")
            params.append(h_min)
        if h_max is not None:
            where.append("h_nom <= ?")
            params.append(h_max)
        if p_min is not None:
            where.append("power_kw >= ?")
            params.append(p_min)
        if p_max is not None:
            where.append("power_kw <= ?")
            params.append(p_max)
        if model:
            where.append("model LIKE ?")
            params.append(f"%{model}%")
        if series:
            where.append("series LIKE ?")
            params.append(f"%{series}%")
        if manufacturer:
            where.append("manufacturer LIKE ?")
            params.append(f"%{manufacturer}%")
        if min_confidence > 0:
            where.append("confidence >= ?")
            params.append(min_confidence)
        if has_curve is True:
            where.append("q_points != '[]' AND h_points != '[]'")
        elif has_curve is False:
            where.append("(q_points = '[]' OR h_points = '[]')")

        where_clause = " AND ".join(where)

        # Count
        count_row = self._get_conn().execute(
            f"SELECT COUNT(*) as cnt FROM pumps WHERE {where_clause}", params
        ).fetchone()
        total = count_row["cnt"]

        # Results
        rows = self._get_conn().execute(
            f"SELECT * FROM pumps WHERE {where_clause} ORDER BY model LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        return [self._pump_row_to_dict(r) for r in rows], total

    def count_pumps(self) -> int:
        """Total pump count across all catalogs."""
        row = self._get_conn().execute("SELECT COUNT(*) as cnt FROM pumps").fetchone()
        return row["cnt"]

    # ─── Job tracking ─────────────────────────────────────────────────────

    def save_job(self, job_id: str, filename: str, status: str = "pending",
                 catalog_id: int | None = None, error: str | None = None) -> None:
        """Save or update a parse job."""
        now = datetime.now(timezone.utc).isoformat()
        with self._tx() as conn:
            conn.execute(
                """INSERT INTO jobs (id, filename, status, started_at, catalog_id, error)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                    status=excluded.status,
                    completed_at=CASE WHEN excluded.status IN ('completed','failed') THEN ? ELSE completed_at END,
                    catalog_id=COALESCE(excluded.catalog_id, catalog_id),
                    error=excluded.error
                """,
                (job_id, filename, status, now, catalog_id, error, now),
            )

    def get_job(self, job_id: str) -> dict | None:
        row = self._get_conn().execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None

    # ─── Stats ────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Global database statistics."""
        conn = self._get_conn()
        catalogs = conn.execute("SELECT COUNT(*) as cnt FROM catalogs").fetchone()["cnt"]
        pumps = conn.execute("SELECT COUNT(*) as cnt FROM pumps").fetchone()["cnt"]
        avg_conf = conn.execute("SELECT AVG(confidence) as avg FROM pumps").fetchone()["avg"] or 0.0
        with_curve = conn.execute(
            "SELECT COUNT(*) as cnt FROM pumps WHERE q_points != '[]' AND h_points != '[]'"
        ).fetchone()["cnt"]

        return {
            "total_catalogs": catalogs,
            "total_pumps": pumps,
            "avg_confidence": round(avg_conf, 3),
            "pumps_with_curve": with_curve,
        }

    # ─── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _pump_row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["q_points"] = json.loads(d.get("q_points", "[]"))
        d["h_points"] = json.loads(d.get("h_points", "[]"))
        d["warnings"] = json.loads(d.get("warnings", "[]"))
        return d
