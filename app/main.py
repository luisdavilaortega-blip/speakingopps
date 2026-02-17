import os
from datetime import date
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

app = FastAPI(title="Speaking Opportunity Finder")

def _normalize_database_url(url: str) -> str:
    # Render provides DATABASE_URL like postgres://... which SQLAlchemy expects as postgresql://...
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url

def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # Local fallback for quick testing
        db_url = "sqlite:///./opportunities.db"
    db_url = _normalize_database_url(db_url)
    connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
    return create_engine(db_url, pool_pre_ping=True, connect_args=connect_args)

engine = get_engine()

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS opportunities (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  organizer TEXT,
  url TEXT UNIQUE NOT NULL,
  location TEXT,
  is_remote BOOLEAN DEFAULT FALSE,
  topic_tags TEXT,
  cfp_deadline DATE,
  event_date DATE,
  source TEXT,
  last_seen DATE NOT NULL DEFAULT CURRENT_DATE
);
"""

def init_db():
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL))

init_db()

def fetch_opportunities(
    q: Optional[str] = None,
    tag: Optional[str] = None,
    remote: Optional[bool] = None,
    limit: int = 50,
):
    where = []
    params = {"limit": limit}

    if q:
        where.append("(LOWER(title) LIKE :q OR LOWER(organizer) LIKE :q)")
        params["q"] = f"%{q.lower()}%"

    if tag:
        where.append("(LOWER(topic_tags) LIKE :tag)")
        params["tag"] = f"%{tag.lower()}%"

    if remote is not None:
        where.append("(is_remote = :remote)")
        params["remote"] = remote

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
    SELECT id, title, organizer, url, location, is_remote, topic_tags, cfp_deadline, event_date, source, last_seen
    FROM opportunities
    {where_clause}
    ORDER BY
      CASE WHEN cfp_deadline IS NULL THEN 1 ELSE 0 END,
      cfp_deadline ASC,
      last_seen DESC
    LIMIT :limit;
    """

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]

@app.get("/", response_class=HTMLResponse)
def home(
    q: Optional[str] = Query(default=None),
    tag: Optional[str] = Query(default=None),
    remote: Optional[str] = Query(default=None),  # "yes" / "no" / None
):
    remote_bool = None
    if remote == "yes":
        remote_bool = True
    elif remote == "no":
        remote_bool = False

    results = fetch_opportunities(q=q, tag=tag, remote=remote_bool, limit=50)

    def esc(s: Optional[str]) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows_html = ""
    for r in results:
        deadline = r["cfp_deadline"] or ""
        edate = r["event_date"] or ""
        tags = r["topic_tags"] or ""
        loc = r["location"] or ("Remote" if r["is_remote"] else "")
        rows_html += f"""
        <tr>
          <td><a href="{esc(r["url"])}" target="_blank" rel="noreferrer">{esc(r["title"])}</a></td>
          <td>{esc(r.get("organizer") or "")}</td>
          <td>{esc(loc)}</td>
          <td>{esc(tags)}</td>
          <td>{deadline}</td>
          <td>{edate}</td>
          <td>{esc(r.get("source") or "")}</td>
        </tr>
        """

    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>Speaking Opportunity Finder</title>
        <style>
          body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Arial; margin: 24px; }}
          .card {{ border: 1px solid #ddd; border-radius: 12px; padding: 16px; max-width: 1100px; }}
          .row {{ display: flex; gap: 12px; flex-wrap: wrap; }}
          input, select {{ padding: 10px; border-radius: 10px; border: 1px solid #ccc; min-width: 220px; }}
          button {{ padding: 10px 14px; border-radius: 10px; border: 1px solid #333; background: #111; color: #fff; cursor: pointer; }}
          table {{ width: 100%; border-collapse: collapse; margin-top: 14px; }}
          th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #eee; vertical-align: top; }}
          th {{ font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: #555; }}
          .hint {{ color: #666; font-size: 13px; margin-top: 8px; }}
        </style>
      </head>
      <body>
        <h1>Speaking Opportunity Finder</h1>
        <div class="card">
          <form method="get">
            <div class="row">
              <input name="q" placeholder="Search (title or organizer)" value="{esc(q)}"/>
              <input name="tag" placeholder="Tag (e.g., AI, sustainability)" value="{esc(tag)}"/>
              <select name="remote">
                <option value="" {"selected" if remote is None else ""}>Remote or in-person (any)</option>
                <option value="yes" {"selected" if remote == "yes" else ""}>Remote only</option>
                <option value="no" {"selected" if remote == "no" else ""}>In-person only</option>
              </select>
              <button type="submit">Search</button>
            </div>
          </form>
          <div class="hint">
            This is a starter prototype. Next we’ll plug in real sources and refresh on a schedule.
          </div>
          <table>
            <thead>
              <tr>
                <th>Opportunity</th>
                <th>Organizer</th>
                <th>Location</th>
                <th>Tags</th>
                <th>CFP deadline</th>
                <th>Event date</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {rows_html if rows_html else "<tr><td colspan='7'>No results yet. (We’ll add sample data in the next step.)</td></tr>"}
            </tbody>
          </table>
        </div>
      </body>
    </html>
    """
    return HTMLResponse(html)

@app.get("/api/opportunities")
def api_opportunities(
    q: Optional[str] = None,
    tag: Optional[str] = None,
    remote: Optional[bool] = None,
    limit: int = 50,
):
    results = fetch_opportunities(q=q, tag=tag, remote=remote, limit=limit)
    return JSONResponse({"count": len(results), "results": results})
