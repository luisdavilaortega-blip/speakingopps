import os
from datetime import date
from sqlalchemy import create_engine, text

def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url

DB_URL = os.getenv("DATABASE_URL", "sqlite:///./opportunities.db")
engine = create_engine(_normalize_database_url(DB_URL), pool_pre_ping=True)

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

UPSERT_SQL = """
INSERT INTO opportunities (title, organizer, url, location, is_remote, topic_tags, cfp_deadline, event_date, source, last_seen)
VALUES (:title, :organizer, :url, :location, :is_remote, :topic_tags, :cfp_deadline, :event_date, :source, :last_seen)
ON CONFLICT (url) DO UPDATE SET
  title = EXCLUDED.title,
  organizer = EXCLUDED.organizer,
  location = EXCLUDED.location,
  is_remote = EXCLUDED.is_remote,
  topic_tags = EXCLUDED.topic_tags,
  cfp_deadline = EXCLUDED.cfp_deadline,
  event_date = EXCLUDED.event_date,
  source = EXCLUDED.source,
  last_seen = EXCLUDED.last_seen;
"""

def main():
    today = date.today()

    sample = [
        {
            "title": "Call for Speakers: AI Infrastructure Summit",
            "organizer": "Example Events",
            "url": "https://example.com/cfp/ai-infra",
            "location": "San Francisco, CA",
            "is_remote": False,
            "topic_tags": "AI, data centers, infrastructure",
            "cfp_deadline": today.replace(day=min(today.day, 28)),
            "event_date": None,
            "source": "sample",
            "last_seen": today,
        },
        {
            "title": "Webinar Speakers Needed: Sustainable Energy Tech",
            "organizer": "Example Webinar Org",
            "url": "https://example.com/cfp/sustainability-webinar",
            "location": "Remote",
            "is_remote": True,
            "topic_tags": "sustainability, energy, grid",
            "cfp_deadline": None,
            "event_date": None,
            "source": "sample",
            "last_seen": today,
        },
        {
            "title": "Panel Opportunity: Cybersecurity in Industrial Systems",
            "organizer": "Example Conference",
            "url": "https://example.com/cfp/ics-security",
            "location": "Chicago, IL",
            "is_remote": False,
            "topic_tags": "cybersecurity, industrial, OT",
            "cfp_deadline": None,
            "event_date": None,
            "source": "sample",
            "last_seen": today,
        },
    ]

    with engine.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
        for item in sample:
            conn.execute(text(UPSERT_SQL), item)

    print(f"Inserted/updated {len(sample)} items.")

if __name__ == "__main__":
    main()
