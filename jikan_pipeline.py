"""
Jikan Anime Data Pipeline — Fetch → Clean → Store in SQLite
Fetches top anime from Jikan API (MyAnimeList) and stores in anime.db with 7 tables.
No API key needed — Jikan is completely free and open!

✅ RESUME SUPPORT: If you stop and run again, it skips already fetched anime!

Tables created:
    1. anime              → main anime info
    2. anime_genres       → genres per anime
    3. anime_themes       → themes per anime
    4. anime_demographics → demographic per anime
    5. anime_characters   → main characters per anime
    6. anime_staff        → directors per anime
    7. anime_statistics   → watch statistics per anime
"""

import time
import sqlite3
import requests
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────
BASE  = "https://api.jikan.moe/v4"
DB    = "anime.db"
PAGES = 10       # 25 anime per page → 250 anime total
SLEEP = 0.5      # 0.5s between requests = 2 req/sec (safe limit is 3)

print("=" * 60)
print("  JIKAN ANIME DATA PIPELINE")
print("=" * 60)
print(f"  Target  : {PAGES * 25} anime from top list")
print(f"  Database: {DB}")
print(f"  Sleep   : {SLEEP}s between requests")
print("=" * 60)

# ── Database Setup ──────────────────────────────────────────────────────
conn   = sqlite3.connect(DB)
cursor = conn.cursor()

cursor.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS anime (
        mal_id          INTEGER PRIMARY KEY,
        title           TEXT NOT NULL,
        title_english   TEXT,
        title_japanese  TEXT,
        type            TEXT,
        source          TEXT,
        episodes        INTEGER,
        status          TEXT,
        duration        TEXT,
        rating          TEXT,
        score           REAL,
        scored_by       INTEGER,
        rank            INTEGER,
        popularity      INTEGER,
        members         INTEGER,
        favorites       INTEGER,
        season          TEXT,
        year            INTEGER,
        aired_from      TEXT,
        aired_to        TEXT,
        studio          TEXT,
        broadcast_day   TEXT,
        broadcast_time  TEXT,
        synopsis        TEXT,
        fetched_at      TEXT
    );

    CREATE TABLE IF NOT EXISTS anime_genres (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        mal_id      INTEGER,
        genre_name  TEXT,
        FOREIGN KEY (mal_id) REFERENCES anime(mal_id)
    );

    CREATE TABLE IF NOT EXISTS anime_themes (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        mal_id      INTEGER,
        theme_name  TEXT,
        FOREIGN KEY (mal_id) REFERENCES anime(mal_id)
    );

    CREATE TABLE IF NOT EXISTS anime_demographics (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        mal_id           INTEGER,
        demographic_name TEXT,
        FOREIGN KEY (mal_id) REFERENCES anime(mal_id)
    );

    CREATE TABLE IF NOT EXISTS anime_characters (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        mal_id         INTEGER,
        character_id   INTEGER,
        character_name TEXT,
        role           TEXT,
        favorites      INTEGER,
        FOREIGN KEY (mal_id) REFERENCES anime(mal_id)
    );

    CREATE TABLE IF NOT EXISTS anime_staff (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        mal_id        INTEGER,
        person_name   TEXT,
        position      TEXT,
        FOREIGN KEY (mal_id) REFERENCES anime(mal_id)
    );

    CREATE TABLE IF NOT EXISTS anime_statistics (
        mal_id          INTEGER PRIMARY KEY,
        watching        INTEGER,
        completed       INTEGER,
        on_hold         INTEGER,
        dropped         INTEGER,
        plan_to_watch   INTEGER,
        total           INTEGER,
        FOREIGN KEY (mal_id) REFERENCES anime(mal_id)
    );
""")
conn.commit()
print("\n✅ Database and all 7 tables ready!")

# ── Helper Functions ─────────────────────────────────────────────────────

def safe_get(url, params=None, retries=3):
    """Make a GET request with retry logic."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                print(f"\n  ⚠️  Rate limited! Sleeping 15 seconds...")
                time.sleep(15)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            print(f"\n  ⚠️  Timeout on attempt {attempt+1}, retrying...")
            time.sleep(2)
        except Exception as e:
            print(f"\n  ⚠️  Error: {e}")
            time.sleep(2)
    return None


def anime_basic_exists(mal_id):
    """Check if anime basic info already in DB."""
    return cursor.execute(
        "SELECT 1 FROM anime WHERE mal_id=?", (mal_id,)
    ).fetchone() is not None


def stats_exists(mal_id):
    """Check if statistics already fetched for this anime."""
    return cursor.execute(
        "SELECT 1 FROM anime_statistics WHERE mal_id=?", (mal_id,)
    ).fetchone() is not None


def characters_exist(mal_id):
    """Check if characters already fetched for this anime."""
    return cursor.execute(
        "SELECT 1 FROM anime_characters WHERE mal_id=?", (mal_id,)
    ).fetchone() is not None


def staff_exists(mal_id):
    """Check if staff already fetched for this anime."""
    return cursor.execute(
        "SELECT 1 FROM anime_staff WHERE mal_id=?", (mal_id,)
    ).fetchone() is not None


def insert_anime(data):
    """Insert one anime into the anime table."""
    mid = data["mal_id"]

    studio = None
    if data.get("studios"):
        studio = data["studios"][0]["name"]

    broadcast      = data.get("broadcast", {})
    broadcast_day  = broadcast.get("day")
    broadcast_time = broadcast.get("time")

    aired      = data.get("aired", {})
    aired_from = aired.get("from")
    aired_to   = aired.get("to")

    cursor.execute("""
        INSERT OR REPLACE INTO anime (
            mal_id, title, title_english, title_japanese,
            type, source, episodes, status, duration, rating,
            score, scored_by, rank, popularity, members, favorites,
            season, year, aired_from, aired_to,
            studio, broadcast_day, broadcast_time, synopsis, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        mid,
        data.get("title"),
        data.get("title_english"),
        data.get("title_japanese"),
        data.get("type"),
        data.get("source"),
        data.get("episodes"),
        data.get("status"),
        data.get("duration"),
        data.get("rating"),
        data.get("score"),
        data.get("scored_by"),
        data.get("rank"),
        data.get("popularity"),
        data.get("members"),
        data.get("favorites"),
        data.get("season"),
        data.get("year"),
        aired_from,
        aired_to,
        studio,
        broadcast_day,
        broadcast_time,
        data.get("synopsis"),
        datetime.now().isoformat()
    ))

    # Insert genres only if not already there
    existing = cursor.execute(
        "SELECT 1 FROM anime_genres WHERE mal_id=?", (mid,)
    ).fetchone()

    if not existing:
        for genre in data.get("genres", []):
            cursor.execute(
                "INSERT INTO anime_genres (mal_id, genre_name) VALUES (?,?)",
                (mid, genre["name"])
            )
        for theme in data.get("themes", []):
            cursor.execute(
                "INSERT INTO anime_themes (mal_id, theme_name) VALUES (?,?)",
                (mid, theme["name"])
            )
        for demo in data.get("demographics", []):
            cursor.execute(
                "INSERT INTO anime_demographics (mal_id, demographic_name) VALUES (?,?)",
                (mid, demo["name"])
            )


def insert_characters(mal_id, data):
    """Insert top 10 characters for one anime."""
    count = 0
    for char in data.get("data", []):
        if count >= 10:
            break
        character = char.get("character", {})
        cursor.execute("""
            INSERT INTO anime_characters
            (mal_id, character_id, character_name, role, favorites)
            VALUES (?,?,?,?,?)
        """, (
            mal_id,
            character.get("mal_id"),
            character.get("name"),
            char.get("role"),
            char.get("favorites", 0)
        ))
        count += 1


def insert_staff(mal_id, data):
    """Insert only Director from staff list."""
    for member in data.get("data", []):
        positions = member.get("positions", [])
        if "Director" in positions:
            person = member.get("person", {})
            cursor.execute("""
                INSERT INTO anime_staff (mal_id, person_name, position)
                VALUES (?,?,?)
            """, (mal_id, person.get("name"), "Director"))
            break


def insert_statistics(mal_id, data):
    """Insert watch statistics."""
    stats = data.get("data", {})
    cursor.execute("""
        INSERT OR REPLACE INTO anime_statistics
        (mal_id, watching, completed, on_hold, dropped, plan_to_watch, total)
        VALUES (?,?,?,?,?,?,?)
    """, (
        mal_id,
        stats.get("watching", 0),
        stats.get("completed", 0),
        stats.get("on_hold", 0),
        stats.get("dropped", 0),
        stats.get("plan_to_watch", 0),
        stats.get("total", 0)
    ))


# ── Pass 1: Fetch Top Anime List ─────────────────────────────────────────
print(f"\n{'─'*60}")
print(f"  PASS 1: Fetching top anime list ({PAGES} pages × 25 = {PAGES*25} anime)")
print(f"{'─'*60}")

all_anime = {}

for page in range(1, PAGES + 1):
    print(f"  Page {page:>2}/{PAGES}...", end=" ", flush=True)
    data = safe_get(f"{BASE}/top/anime", params={"page": page})
    if data and "data" in data:
        for anime in data["data"]:
            all_anime[anime["mal_id"]] = anime
        print(f"✅  Total unique: {len(all_anime)}")
    else:
        print("❌ Failed to fetch")
    time.sleep(SLEEP)

print(f"\n✅ Pass 1 done! {len(all_anime)} anime discovered.\n")

# ── Pass 2: Fetch Details for Each Anime ────────────────────────────────
print(f"{'─'*60}")
print(f"  PASS 2: Fetching characters, staff & statistics")
print(f"  ⏭️  Already fetched anime will be SKIPPED automatically")
print(f"{'─'*60}\n")

anime_ids = list(all_anime.keys())
total   = len(anime_ids)
success = skipped = errors = 0

for i, mal_id in enumerate(anime_ids):

    # Check what is already done for this anime
    basic_done = anime_basic_exists(mal_id)
    chars_done = characters_exist(mal_id)
    staff_done = staff_exists(mal_id)
    stats_done = stats_exists(mal_id)

    # If everything already done → skip entirely
    if basic_done and chars_done and staff_done and stats_done:
        skipped += 1
        if (i + 1) % 25 == 0 or (i + 1) == total:
            pct = (i + 1) / total * 100
            print(f"  [{pct:>5.1f}%]  ✅ {success} fetched  ⏭️  {skipped} skipped  ❌ {errors} errors")
        continue

    # Insert basic anime info if not done
    if not basic_done:
        try:
            insert_anime(all_anime[mal_id])
        except Exception as e:
            print(f"\n  ❌ Error inserting anime {mal_id}: {e}")
            errors += 1
            continue

    # Fetch characters if not done
    if not chars_done:
        char_data = safe_get(f"{BASE}/anime/{mal_id}/characters")
        if char_data:
            insert_characters(mal_id, char_data)
        time.sleep(SLEEP)

    # Fetch staff if not done
    if not staff_done:
        staff_data = safe_get(f"{BASE}/anime/{mal_id}/staff")
        if staff_data:
            insert_staff(mal_id, staff_data)
        time.sleep(SLEEP)

    # Fetch statistics if not done
    if not stats_done:
        stats_data = safe_get(f"{BASE}/anime/{mal_id}/statistics")
        if stats_data:
            insert_statistics(mal_id, stats_data)
        time.sleep(SLEEP)

    conn.commit()
    success += 1

    if (i + 1) % 25 == 0 or (i + 1) == total:
        pct = (i + 1) / total * 100
        print(f"  [{pct:>5.1f}%]  ✅ {success} fetched  ⏭️  {skipped} skipped  ❌ {errors} errors")

print(f"\n✅ Pass 2 complete!")

# ── Final Summary ────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("  FINAL DATABASE SUMMARY")
print(f"{'='*60}")
print(f"  {'Table':<25} {'Rows':>8}")
print(f"  {'─'*35}")

for table in ["anime", "anime_genres", "anime_themes",
              "anime_demographics", "anime_characters",
              "anime_staff", "anime_statistics"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table:<25} {count:>8,}")

print(f"\n{'─'*60}")
print("  QUICK STATS")
print(f"{'─'*60}")

total_anime = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
avg_score   = conn.execute("SELECT ROUND(AVG(score),2) FROM anime WHERE score IS NOT NULL").fetchone()[0]
top_genre   = conn.execute("""
    SELECT genre_name, COUNT(*) as cnt FROM anime_genres
    GROUP BY genre_name ORDER BY cnt DESC LIMIT 1
""").fetchone()
top_studio  = conn.execute("""
    SELECT studio, COUNT(*) as cnt FROM anime
    WHERE studio IS NOT NULL
    GROUP BY studio ORDER BY cnt DESC LIMIT 1
""").fetchone()

print(f"  Total anime         : {total_anime:,}")
print(f"  Average score       : {avg_score}")
if top_genre:
    print(f"  Most common genre   : {top_genre[0]} ({top_genre[1]} anime)")
if top_studio:
    print(f"  Most active studio  : {top_studio[0]} ({top_studio[1]} anime)")

conn.close()

print(f"\n{'='*60}")
print(f"  ✅ Done! Database saved as: {DB}")
print(f"  Next step → run jikan_eda.py for analysis!")
print(f"{'='*60}\n")
