from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

API_TOKEN = os.environ.get("API_TOKEN", "dev-token")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
# throttle OpenAI calls to avoid 429s when processing many leads
OPENAI_MIN_INTERVAL_MS = int(os.environ.get("OPENAI_MIN_INTERVAL_MS", "750"))
OPENAI_MAX_RETRIES = int(os.environ.get("OPENAI_MAX_RETRIES", "3"))

# Lovable-friendly CORS:
# - Set CORS_ALLOW_ORIGINS="https://your-lovable-app.com,https://another-origin.com"
# - For demo convenience, default is "*".
CORS_ALLOW_ORIGINS = os.environ.get("CORS_ALLOW_ORIGINS", "*")

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "poc.db")

security = HTTPBearer(auto_error=False)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def auth(creds: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    if not creds or not creds.credentials or creds.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    cur = conn.cursor()
    cur.executescript(
        """
        create table if not exists campaign_runs (
          id text primary key,
          name text,
          status text,
          criteria_json text,
          progress_json text,
          created_at text,
          updated_at text
        );

        create table if not exists leads (
          id text primary key,
          campaign_run_id text,
          seed_json text,
          lead_json text,
          score_total integer,
          tier text,
          created_at text,
          updated_at text
        );

        create table if not exists tasks (
          id text primary key,
          campaign_run_id text,
          lead_id text,
          type text,
          channel text,
          status text,
          due_at_est text,
          window_start_est text,
          window_end_est text,
          instructions text,
          template_id text,
          completion_json text,
          created_at text,
          updated_at text
        );

        create table if not exists events (
          id text primary key,
          campaign_run_id text,
          lead_id text,
          time text,
          type text,
          message text,
          payload_json text
        );

        create table if not exists places_cache (
          cache_key text primary key,
          name text,
          city text,
          state text,
          website_hint text,
          status text,
          place_id text,
          maps_url text,
          rating real,
          reviews integer,
          types_json text,
          phone text,
          website text,
          formatted_address text,
          lat real,
          lng real,
          raw_json text,
          fetched_at text
        );

        create table if not exists outreach_attempts (
          id text primary key,
          campaign_run_id text,
          lead_id text,
          channel text,
          template_id text,
          executed_at text,
          outcome_code text,
          outcome_notes text,
          created_at text
        );

        create table if not exists business_profiles (
          id text primary key,
          lead_id text unique,
          vertical_category text,
          services_json text,
          customer_type text,
          buyer_persona_hint text,
          credibility_signals_json text,
          confidence real,
          evidence_used_json text,
          raw_llm_json text,
          created_at text,
          updated_at text
        );

        create table if not exists message_drafts (
          id text primary key,
          campaign_run_id text,
          lead_id text,
          channel text,
          template_id text,
          subject text,
          body text,
          personalization_facts_json text,
          safety_checks_passed integer,
          created_at text
        );

        create table if not exists tactics (
          id text primary key,
          campaign_run_id text,
          name text,
          description text,
          channels_json text,
          target_segment_json text,
          hypothesis text,
          pattern_json text,
          created_by text,
          status text,
          created_at text,
          updated_at text
        );

        create table if not exists experiments (
          id text primary key,
          campaign_run_id text,
          tactic_id text,
          allocation_pct integer,
          started_at text,
          ended_at text,
          metrics_snapshot_json text,
          status text
        );
        """
    )

    def _ensure_column(table: str, column: str, coltype: str):
        cols = [r["name"] for r in conn.execute(f"pragma table_info({table})").fetchall()]
        if column in cols:
            return
        conn.execute(f"alter table {table} add column {column} {coltype}")

    # Lightweight migrations for new attribution columns
    _ensure_column("tasks", "tactic_id", "text")
    _ensure_column("tasks", "experiment_id", "text")
    _ensure_column("outreach_attempts", "tactic_id", "text")
    _ensure_column("outreach_attempts", "experiment_id", "text")

    conn.commit()
    conn.close()


def emit_event(campaign_run_id: str, type_: str, message: str, payload: Optional[Dict[str, Any]] = None, lead_id: Optional[str] = None):
    conn = db()
    cur = conn.cursor()
    eid = f"evt_{uuid.uuid4().hex}"
    cur.execute(
        "insert into events (id, campaign_run_id, lead_id, time, type, message, payload_json) values (?,?,?,?,?,?,?)",
        (eid, campaign_run_id, lead_id, now_iso(), type_, message, json.dumps(payload or {})),
    )
    conn.commit()
    conn.close()


def get_campaign_run(campaign_run_id: str) -> Dict[str, Any]:
    conn = db()
    row = conn.execute("select * from campaign_runs where id=?", (campaign_run_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Campaign run not found")
    return {
        "id": row["id"],
        "name": row["name"],
        "status": row["status"],
        "criteria": json.loads(row["criteria_json"]),
        "progress": json.loads(row["progress_json"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def update_campaign_run(campaign_run_id: str, *, status: Optional[str] = None, progress: Optional[Dict[str, Any]] = None):
    conn = db()
    row = conn.execute("select * from campaign_runs where id=?", (campaign_run_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Campaign run not found")
    new_status = status or row["status"]
    new_progress = progress or json.loads(row["progress_json"])
    conn.execute(
        "update campaign_runs set status=?, progress_json=?, updated_at=? where id=?",
        (new_status, json.dumps(new_progress), now_iso(), campaign_run_id),
    )
    conn.commit()
    conn.close()


def get_campaign_run_status(campaign_run_id: str) -> Optional[str]:
    """Fast status check used by the background runner."""
    conn = db()
    row = conn.execute("select status from campaign_runs where id=?", (campaign_run_id,)).fetchone()
    conn.close()
    return (row["status"] if row else None)


class CampaignRunCreate(BaseModel):
    name: str
    criteria: Dict[str, Any]


class OutreachAttemptCreate(BaseModel):
    campaign_run_id: str
    lead_id: str
    channel: str  # email|phone|linkedin|other
    template_id: Optional[str] = None
    executed_at: str
    outcome_code: str  # sent|delivered|replied|positive|negative|meeting_booked|bounced|unsubscribed
    outcome_notes: Optional[str] = None
    # Attribution (optional for now)
    tactic_id: Optional[str] = None
    experiment_id: Optional[str] = None


app = FastAPI(title="Lovable Agentic API (POC)")

_allow_origins = [o.strip() for o in (CORS_ALLOW_ORIGINS or "").split(",") if o.strip()]
if not _allow_origins:
    _allow_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type"],
    expose_headers=["Content-Type"],
)


@app.on_event("startup")
def _startup():
    init_db()


@app.get("/v1/health")
def health_root():
    return {
        "ok": True,
        "service": "Lovable Agentic API (POC)",
        "time": now_iso(),
    }


@app.get("/v1/health/openai", dependencies=[Depends(auth)])
def health_openai(probe: bool = False):
    """OpenAI integration health.

    - configured: whether OPENAI_API_KEY is set
    - probe=true: makes a tiny real call to verify credentials + network
    """
    out: Dict[str, Any] = {
        "configured": bool(OPENAI_API_KEY),
        "model": OPENAI_MODEL,
    }
    if not probe:
        return out

    if not OPENAI_API_KEY:
        return {**out, "ok": False, "error": "OPENAI_API_KEY not set"}

    t0 = time.time()
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": "Return valid JSON only."},
                    {"role": "user", "content": "{\"ping\":true}"},
                ],
                "response_format": {"type": "json_object"},
                "max_completion_tokens": 30,
            },
            timeout=20,
        )
        ok = r.status_code == 200
        # do not return full response (could include provider metadata); just summary
        err = None
        if not ok:
            try:
                err = (r.json() or {}).get("error")
            except Exception:
                err = r.text[:500]
        dt = int((time.time() - t0) * 1000)
        return {**out, "ok": ok, "status_code": r.status_code, "latency_ms": dt, "error": err}
    except Exception as e:
        dt = int((time.time() - t0) * 1000)
        return {**out, "ok": False, "status_code": None, "latency_ms": dt, "error": f"{type(e).__name__}: {str(e)[:300]}"}


@app.post("/v1/campaign-runs", dependencies=[Depends(auth)])
def create_campaign_run(body: CampaignRunCreate):
    cid = f"cr_{uuid.uuid4().hex}"
    created = now_iso()
    progress = {
        "seed_rows_total": 0,
        "leads_created": 0,
        "leads_enriched_places": 0,
        "leads_enriched_website": 0,
        "leads_scored": 0,
        "tasks_created": 0,
        "tasks_completed": 0,
        # execution metrics (OutreachAttempts)
        "attempts_total": 0,
        "replies": 0,
        "meetings": 0,
        "errors": 0,
    }
    conn = db()
    conn.execute(
        "insert into campaign_runs (id,name,status,criteria_json,progress_json,created_at,updated_at) values (?,?,?,?,?,?,?)",
        (cid, body.name, "draft", json.dumps(body.criteria), json.dumps(progress), created, created),
    )
    conn.commit()
    conn.close()
    emit_event(cid, "progress", "Campaign created", {"progress": progress})
    return get_campaign_run(cid)


def _parse_runs_cursor(cursor: Optional[str]) -> Optional[Tuple[str, str]]:
    """Cursor format: <created_at>|<id>"""
    if not cursor:
        return None
    if "|" not in cursor:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    created_at, rid = cursor.split("|", 1)
    if not created_at or not rid:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return created_at, rid


@app.get("/v1/campaign-runs", dependencies=[Depends(auth)])
def list_campaign_runs(limit: int = 50, cursor: Optional[str] = None, status: Optional[str] = None):
    # newest first
    limit = max(1, min(int(limit or 50), 200))
    statuses = [s.strip() for s in (status or "").split(",") if s.strip()]

    where = "where 1=1"
    params: List[Any] = []
    if statuses:
        where += " and status in (%s)" % ",".join(["?"] * len(statuses))
        params.extend(statuses)

    cur = _parse_runs_cursor(cursor)
    if cur:
        cur_created, cur_id = cur
        # strictly older than cursor tuple (created_at desc, id desc)
        where += " and (created_at < ? or (created_at = ? and id < ?))"
        params.extend([cur_created, cur_created, cur_id])

    conn = db()
    rows = conn.execute(
        f"select * from campaign_runs {where} order by created_at desc, id desc limit ?",
        tuple(params + [limit + 1]),
    ).fetchall()
    conn.close()

    sliced = rows[:limit]
    items = []
    for r in sliced:
        items.append(
            {
                "id": r["id"],
                "name": r["name"],
                "status": r["status"],
                "criteria": json.loads(r["criteria_json"] or "{}"),
                "progress": json.loads(r["progress_json"] or "{}"),
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            }
        )

    next_cursor = None
    if len(rows) > limit and sliced:
        last = sliced[-1]
        next_cursor = f"{last['created_at']}|{last['id']}"

    return {"items": items, "nextCursor": next_cursor}


@app.get("/v1/campaign-runs/{campaign_run_id}", dependencies=[Depends(auth)])
def campaign_run_get(campaign_run_id: str):
    return get_campaign_run(campaign_run_id)


@app.post("/v1/campaign-runs/{campaign_run_id}/abort", dependencies=[Depends(auth)])
def abort_campaign_run(campaign_run_id: str):
    """Abort a stuck/running CampaignRun.

    This is an admin escape hatch for demos:
    - Marks status=aborted
    - Emits a campaign.aborted event

    Notes:
    - If the background runner is still alive, it will notice and exit.
    - If it already crashed, this clears the stuck status so UIs can recover.
    """
    cr = get_campaign_run(campaign_run_id)
    if cr.get("status") == "complete":
        raise HTTPException(status_code=409, detail="Campaign run already complete")

    # idempotent
    if cr.get("status") == "aborted":
        return cr

    update_campaign_run(campaign_run_id, status="aborted")
    emit_event(campaign_run_id, "campaign.aborted", "Campaign aborted", {"status": "aborted"})
    return get_campaign_run(campaign_run_id)


@app.post("/v1/campaign-runs/{campaign_run_id}/import/seed-csv", dependencies=[Depends(auth)])
def import_seed_csv(campaign_run_id: str, file: UploadFile = File(...)):
    data = file.file.read()
    text = data.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    required = [
        "Contact Name",
        "Email",
        "Business Name",
        "Business Description",
        "Website",
        "Status",
        "Last Email Date",
        "Employees",
        "City",
        "State",
    ]
    for r in required:
        if r not in reader.fieldnames:
            raise HTTPException(400, f"Missing required column: {r}")

    conn = db()
    imported = 0
    for row in reader:
        lid = f"ld_{uuid.uuid4().hex}"
        seed = {
            "contact_name": row.get("Contact Name", "").strip(),
            "email": row.get("Email", "").strip(),
            "business_name": row.get("Business Name", "").strip(),
            "business_description": row.get("Business Description", "").strip(),
            "website": row.get("Website", "").strip(),
            "status": row.get("Status", "").strip(),
            "last_email_date": row.get("Last Email Date", "").strip() or None,
            "employees": int(row.get("Employees") or 0),
            "city": row.get("City", "").strip(),
            "state": row.get("State", "").strip(),
        }
        lead_json = {
            "id": lid,
            "campaign_run_id": campaign_run_id,
            "seed": seed,
            "business": {
                "name": seed["business_name"],
                "website": seed["website"],
                "employee_count": seed["employees"],
                "address": {"city": seed["city"], "state": seed["state"]},
            },
            "owner": {
                "full_name": seed["contact_name"],
                "emails": [{"email": seed["email"], "type": "work", "status": "valid", "source": "seed"}],
            },
            "evidence": {"sources": []},
            "extractions": {},
            "score": {"total": None, "tier": None, "components": {}, "reasoning_bullets": []},
            "campaign_plan": {},
            "task_ids": [],
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }
        conn.execute(
            "insert into leads (id,campaign_run_id,seed_json,lead_json,score_total,tier,created_at,updated_at) values (?,?,?,?,?,?,?,?)",
            (lid, campaign_run_id, json.dumps(seed), json.dumps(lead_json), None, None, now_iso(), now_iso()),
        )
        imported += 1
    conn.commit()
    conn.close()

    cr = get_campaign_run(campaign_run_id)
    prog = cr["progress"]
    prog["seed_rows_total"] = imported
    prog["leads_created"] = imported
    update_campaign_run(campaign_run_id, progress=prog)
    emit_event(campaign_run_id, "progress", "Seed CSV imported", {"imported": imported, "progress": prog})

    return {"imported": imported}


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _places_cache_key(name: str, city: str, state: str, website_hint: str = "") -> str:
    # Keep key stable and short. Website hint is optional.
    base = f"{_norm(name)}|{_norm(city)}|{_norm(state)}"
    wh = _norm(website_hint)
    if wh:
        return f"{base}|{wh}"
    return base


def _places_cache_get(cache_key: str, ttl_days: int = 30) -> Optional[Dict[str, Any]]:
    conn = db()
    row = conn.execute("select * from places_cache where cache_key=?", (cache_key,)).fetchone()
    conn.close()
    if not row:
        return None

    fetched_at = row["fetched_at"]
    if fetched_at:
        try:
            dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0
            if age_days > ttl_days:
                return None
        except Exception:
            # if parsing fails, treat as stale
            return None

    if row["status"] != "hit":
        return None

    return {
        "place_id": row["place_id"],
        "url": row["maps_url"],
        "rating": row["rating"],
        "user_ratings_total": row["reviews"],
        "types": json.loads(row["types_json"] or "[]"),
        "formatted_phone_number": row["phone"],
        "website": row["website"],
        "formatted_address": row["formatted_address"],
        "geometry": {"location": {"lat": row["lat"], "lng": row["lng"]}},
    }


def _places_cache_put(cache_key: str, *, name: str, city: str, state: str, website_hint: str, status: str, payload: Dict[str, Any]):
    conn = db()
    types_json = json.dumps(payload.get("types") or [])
    geom = ((payload.get("geometry") or {}).get("location") or {})
    lat = geom.get("lat")
    lng = geom.get("lng")
    conn.execute(
        """
        insert into places_cache (cache_key,name,city,state,website_hint,status,place_id,maps_url,rating,reviews,types_json,phone,website,formatted_address,lat,lng,raw_json,fetched_at)
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        on conflict(cache_key) do update set
          status=excluded.status,
          place_id=excluded.place_id,
          maps_url=excluded.maps_url,
          rating=excluded.rating,
          reviews=excluded.reviews,
          types_json=excluded.types_json,
          phone=excluded.phone,
          website=excluded.website,
          formatted_address=excluded.formatted_address,
          lat=excluded.lat,
          lng=excluded.lng,
          raw_json=excluded.raw_json,
          fetched_at=excluded.fetched_at
        """,
        (
            cache_key,
            name,
            city,
            state,
            website_hint,
            status,
            payload.get("place_id"),
            payload.get("url"),
            payload.get("rating"),
            payload.get("user_ratings_total"),
            types_json,
            payload.get("formatted_phone_number"),
            payload.get("website"),
            payload.get("formatted_address"),
            lat,
            lng,
            json.dumps(payload)[:200_000],
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def _places_lookup(name: str, city: str, state: str, website_hint: str = "") -> Dict[str, Any]:
    # Return shape similar to Google Places Details result.
    if not GOOGLE_MAPS_API_KEY:
        return {}

    cache_key = _places_cache_key(name, city, state, website_hint)
    cached = _places_cache_get(cache_key)
    if cached:
        return cached

    s = requests.Session()
    q = f"{name} {city} {state}".strip()

    # 1) text search
    ts_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    ts = s.get(ts_url, params={"query": q, "key": GOOGLE_MAPS_API_KEY}, timeout=20).json()
    pid = ((ts.get("results") or [{}])[0]).get("place_id")
    if not pid:
        _places_cache_put(cache_key, name=name, city=city, state=state, website_hint=website_hint, status="miss", payload={"place_id": None})
        return {}

    # 2) details
    det_url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = [
        "name",
        "formatted_address",
        "formatted_phone_number",
        "website",
        "url",
        "types",
        "rating",
        "user_ratings_total",
        "opening_hours",
        "geometry",
    ]
    det = s.get(
        det_url,
        params={"place_id": pid, "fields": ",".join(fields), "key": GOOGLE_MAPS_API_KEY},
        timeout=20,
    ).json()
    res = det.get("result") or {}
    res["place_id"] = pid

    # normalize to a small payload we care about, but keep compatibility keys
    out = {
        "place_id": pid,
        "url": res.get("url"),
        "rating": res.get("rating"),
        "user_ratings_total": res.get("user_ratings_total"),
        "types": res.get("types") or [],
        "formatted_phone_number": res.get("formatted_phone_number"),
        "website": res.get("website"),
        "formatted_address": res.get("formatted_address"),
        "opening_hours": res.get("opening_hours") or {},
        "geometry": res.get("geometry") or {},
    }

    _places_cache_put(cache_key, name=name, city=city, state=state, website_hint=website_hint, status="hit", payload={**out, "place_id": pid})
    return out


def _website_fetch(url: str) -> str:
    # Keep this aggressive: website fetch is the #1 source of demo hangs.
    try:
        if not url or not url.startswith(("http://", "https://")):
            return ""
        r = requests.get(
            url,
            timeout=(5, 7),  # connect, read
            headers={"User-Agent": "LovableAgenticPOC/1.0"},
            allow_redirects=True,
        )
        r.raise_for_status()
        return (r.text or "")[:200_000]
    except Exception:
        return ""


def _strip_html(html: str) -> str:
    import re

    html = re.sub(r"(?is)<script.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?</style>", " ", html)
    txt = re.sub(r"(?is)<[^>]+>", " ", html)
    txt = re.sub(r"\s+", " ", txt)
    return txt


BUSINESS_CATEGORIES = [
    "waterproofing",
    "window_cleaning",
    "exterior_painting",
    "roofing",
    "hvac",
    "plumbing",
    "electrical",
    "landscaping",
    "pest_control",
    "janitorial",
    "pressure_washing",
    "concrete",
    "flooring",
    "remodeling",
    "general_contractor",
    "property_management",
    "real_estate_services",
    "moving",
    "security_services",
    "other",
    "unknown",
]


def _bp_default(evidence_used: List[str]) -> Dict[str, Any]:
    return {
        "vertical_category": "unknown",
        "services": [],
        "customer_type": "unknown",
        "buyer_persona_hint": None,
        "credibility_signals": [],
        "confidence": 0.2 if evidence_used else 0.1,
        "evidence_used": evidence_used,
        "raw_llm_json": None,
    }


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """Best-effort: extract first JSON object from a string."""
    import json as _json

    if not text:
        return None
    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        try:
            return _json.loads(text)
        except Exception:
            return None
    # find first { ... }
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return _json.loads(text[start : end + 1])
    except Exception:
        return None


_openai_lock = threading.Lock()
_openai_last_call_at = 0.0


def _openai_throttle():
    global _openai_last_call_at
    if OPENAI_MIN_INTERVAL_MS <= 0:
        return
    with _openai_lock:
        now = time.time()
        wait_s = (_openai_last_call_at + (OPENAI_MIN_INTERVAL_MS / 1000.0)) - now
        if wait_s > 0:
            time.sleep(wait_s)
        _openai_last_call_at = time.time()


def _llm_business_profile(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Call OpenAI if configured; otherwise return defaults.

    Notes:
    - We process leads sequentially, but still throttle to avoid 429s.
    - On 429, we retry with backoff a few times, then default.
    """
    evidence_used = evidence.get("evidence_used") or []
    if not OPENAI_API_KEY:
        return _bp_default(evidence_used)

    system = (
        "You extract a BusinessProfile from provided evidence ONLY. "
        "Do not guess. If not supported, use 'unknown' and lower confidence. "
        "Return VALID JSON only (no markdown, no commentary)."
    )

    user = {
        "task": "Extract BusinessProfile",
        "vertical_category_allowed": BUSINESS_CATEGORIES,
        "schema": {
            "vertical_category": "string",
            "services": "string[]",
            "customer_type": "B2B|B2C|mixed|unknown",
            "buyer_persona_hint": "string|null",
            "credibility_signals": "string[]",
            "confidence": "float 0..1",
            "evidence_used": "string[]",
        },
        "evidence": evidence,
    }

    try:
        last_err: Optional[str] = None
        for attempt in range(OPENAI_MAX_RETRIES):
            _openai_throttle()
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "temperature": 0.2,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": json.dumps(user)},
                    ],
                    "response_format": {"type": "json_object"},
                },
                timeout=45,
            )

            if r.status_code == 429:
                # backoff and retry
                sleep_s = min(8.0, 0.75 * (2**attempt))
                last_err = f"429 rate_limited (attempt {attempt+1}/{OPENAI_MAX_RETRIES})"
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                # non-retryable for this POC
                try:
                    last_err = json.dumps((r.json() or {}).get("error") or {})[:800]
                except Exception:
                    last_err = (r.text or "")[:800]
                break

            data = r.json()
            content = (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
            parsed = _extract_json_object(content)
            if not parsed:
                out = _bp_default(evidence_used)
                out["raw_llm_json"] = content[:50_000]
                return out

            # normalize/guard
            vc = (parsed.get("vertical_category") or "unknown").strip()
            if vc not in BUSINESS_CATEGORIES:
                vc = "unknown"
            conf = parsed.get("confidence")
            try:
                conf_f = float(conf)
            except Exception:
                conf_f = 0.2
            conf_f = max(0.0, min(1.0, conf_f))

            return {
                "vertical_category": vc,
                "services": [str(x) for x in (parsed.get("services") or [])][:50],
                "customer_type": (parsed.get("customer_type") or "unknown"),
                "buyer_persona_hint": parsed.get("buyer_persona_hint"),
                "credibility_signals": [str(x) for x in (parsed.get("credibility_signals") or [])][:50],
                "confidence": conf_f,
                "evidence_used": [str(x) for x in (parsed.get("evidence_used") or evidence_used)],
                "raw_llm_json": content[:50_000],
            }

        # fallthrough: default
        out = _bp_default(evidence_used)
        out["raw_llm_json"] = f"error: {last_err or 'openai_failed'}"
        return out

        # normalize/guard
        vc = (parsed.get("vertical_category") or "unknown").strip()
        if vc not in BUSINESS_CATEGORIES:
            vc = "unknown"
        conf = parsed.get("confidence")
        try:
            conf_f = float(conf)
        except Exception:
            conf_f = 0.2
        conf_f = max(0.0, min(1.0, conf_f))

        return {
            "vertical_category": vc,
            "services": [str(x) for x in (parsed.get("services") or [])][:50],
            "customer_type": (parsed.get("customer_type") or "unknown"),
            "buyer_persona_hint": parsed.get("buyer_persona_hint"),
            "credibility_signals": [str(x) for x in (parsed.get("credibility_signals") or [])][:50],
            "confidence": conf_f,
            "evidence_used": [str(x) for x in (parsed.get("evidence_used") or evidence_used)],
            "raw_llm_json": content[:50_000],
        }
    except Exception as e:
        out = _bp_default(evidence_used)
        out["raw_llm_json"] = f"error: {type(e).__name__}: {str(e)[:400]}"
        return out


def upsert_business_profile(lead_id: str, bp: Dict[str, Any]) -> Dict[str, Any]:
    """Persist BusinessProfile (1:1 with lead)."""
    conn = db()
    row = conn.execute("select id from business_profiles where lead_id=?", (lead_id,)).fetchone()
    now = now_iso()
    if row:
        bp_id = row["id"]
        conn.execute(
            "update business_profiles set vertical_category=?, services_json=?, customer_type=?, buyer_persona_hint=?, credibility_signals_json=?, confidence=?, evidence_used_json=?, raw_llm_json=?, updated_at=? where lead_id=?",
            (
                bp.get("vertical_category"),
                json.dumps(bp.get("services") or []),
                bp.get("customer_type"),
                bp.get("buyer_persona_hint"),
                json.dumps(bp.get("credibility_signals") or []),
                float(bp.get("confidence") or 0.0),
                json.dumps(bp.get("evidence_used") or []),
                json.dumps(bp.get("raw_llm_json")) if isinstance(bp.get("raw_llm_json"), (dict, list)) else (bp.get("raw_llm_json") or ""),
                now,
                lead_id,
            ),
        )
    else:
        bp_id = f"bp_{uuid.uuid4().hex}"
        conn.execute(
            "insert into business_profiles (id, lead_id, vertical_category, services_json, customer_type, buyer_persona_hint, credibility_signals_json, confidence, evidence_used_json, raw_llm_json, created_at, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                bp_id,
                lead_id,
                bp.get("vertical_category"),
                json.dumps(bp.get("services") or []),
                bp.get("customer_type"),
                bp.get("buyer_persona_hint"),
                json.dumps(bp.get("credibility_signals") or []),
                float(bp.get("confidence") or 0.0),
                json.dumps(bp.get("evidence_used") or []),
                json.dumps(bp.get("raw_llm_json")) if isinstance(bp.get("raw_llm_json"), (dict, list)) else (bp.get("raw_llm_json") or ""),
                now,
                now,
            ),
        )

    conn.commit()
    conn.close()
    bp_out = dict(bp)
    bp_out["id"] = bp_id
    bp_out["lead_id"] = lead_id
    return bp_out


def _llm_ai_score_lead(payload: Dict[str, Any]) -> Dict[str, Any]:
    """AI-assisted scoring.

    Returns:
      { ai_used, ai_total, reasoning_bullets, reasoning_citations }

    Demo-safe constraints:
    - Strict JSON only
    - Reasons must cite only provided payload fields
    - On failure, caller should fall back to rules_total
    """
    if not OPENAI_API_KEY:
        return {
            "ai_used": False,
            "ai_total": None,
            "reasoning_bullets": [],
            "reasoning_citations": [],
            "raw_llm_json": None,
        }

    system = (
        "You are scoring a business lead for outreach response likelihood. "
        "Use ONLY the provided evidence payload. Do NOT guess. "
        "Return STRICT JSON only (no markdown, no commentary). "
        "Reasoning bullets must be short (max 4) and must be grounded in cited fields."
    )

    user = {
        "task": "AI score lead (0-100) + grounded reasons",
        "rules": {
            "ai_total": "integer 0..100",
            "reasoning_bullets": "string[] max 4",
            "reasoning_citations": "array of {field: string, value: any} referencing ONLY allowed fields",
        },
        "allowed_field_prefixes": [
            "criteria.",
            "rules_score.",
            "business_profile.",
            "seed.",
            "places.",
            "website_sample.",
        ],
        "payload": payload,
    }

    last_err: Optional[str] = None
    try:
        for attempt in range(OPENAI_MAX_RETRIES):
            _openai_throttle()
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "temperature": 0.2,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": json.dumps(user)},
                    ],
                    "response_format": {"type": "json_object"},
                },
                timeout=45,
            )

            if r.status_code == 429:
                sleep_s = min(8.0, 0.75 * (2**attempt))
                last_err = f"429 rate_limited (attempt {attempt+1}/{OPENAI_MAX_RETRIES})"
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                try:
                    last_err = json.dumps((r.json() or {}).get("error") or {})[:800]
                except Exception:
                    last_err = (r.text or "")[:800]
                break

            data = r.json()
            content = (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
            parsed = _extract_json_object(content) or {}

            ai_total = parsed.get("ai_total")
            try:
                ai_total_i = int(float(ai_total))
            except Exception:
                ai_total_i = None

            bullets = parsed.get("reasoning_bullets") or []
            if not isinstance(bullets, list):
                bullets = []
            bullets = [str(b).strip() for b in bullets if str(b).strip()][:4]

            cits = parsed.get("reasoning_citations") or []
            if not isinstance(cits, list):
                cits = []
            norm_cits: List[Dict[str, Any]] = []
            for c in cits[:20]:
                if not isinstance(c, dict):
                    continue
                field = str(c.get("field") or "").strip()
                if not field:
                    continue
                if not any(field.startswith(p) for p in ["criteria.", "rules_score.", "business_profile.", "seed.", "places.", "website_sample."]):
                    continue
                norm_cits.append({"field": field, "value": c.get("value")})

            if ai_total_i is None:
                # treat as invalid
                last_err = "invalid_ai_total"
                break

            ai_total_i = max(0, min(100, ai_total_i))
            return {
                "ai_used": True,
                "ai_total": ai_total_i,
                "reasoning_bullets": bullets,
                "reasoning_citations": norm_cits,
                "raw_llm_json": content[:50_000],
            }

    except Exception as e:
        last_err = f"{type(e).__name__}: {str(e)[:300]}"

    return {
        "ai_used": False,
        "ai_total": None,
        "reasoning_bullets": [],
        "reasoning_citations": [],
        "raw_llm_json": f"error: {last_err or 'openai_failed'}",
    }


def _llm_message_draft(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a personalized message draft.

    Returns strict JSON with:
      subject, body, personalization_facts, safety_checks_passed

    Caller is responsible for fallback + persistence.
    """
    if not OPENAI_API_KEY:
        return {"ok": False, "error": "OPENAI_API_KEY not set", "raw_llm_json": None}

    system = (
        "You write outreach drafts grounded ONLY in the provided evidence. "
        "Do not invent facts. If evidence is thin, keep it generic. "
        "Return STRICT JSON only (no markdown)."
    )

    user = {
        "task": "Generate MessageDraft",
        "channel": payload.get("channel"),
        "template_id": payload.get("template_id"),
        "requirements": {
            "subject": "string (email only)",
            "body": "string",
            "personalization_facts": "string[] (facts actually used)",
            "safety_checks_passed": "boolean (true only if all facts are supported by payload evidence)",
        },
        "allowed_fact_sources": [
            "business_profile",
            "seed.business_name",
            "seed.city",
            "seed.state",
            "places.formatted_address",
            "places.rating",
            "places.reviews",
            "places.website",
            "website_sample",
        ],
        "payload": payload,
    }

    last_err: Optional[str] = None
    try:
        for attempt in range(OPENAI_MAX_RETRIES):
            _openai_throttle()
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "temperature": 0.4,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": json.dumps(user)},
                    ],
                    "response_format": {"type": "json_object"},
                },
                timeout=60,
            )

            if r.status_code == 429:
                sleep_s = min(8.0, 0.75 * (2**attempt))
                last_err = f"429 rate_limited (attempt {attempt+1}/{OPENAI_MAX_RETRIES})"
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                try:
                    last_err = json.dumps((r.json() or {}).get("error") or {})[:800]
                except Exception:
                    last_err = (r.text or "")[:800]
                break

            data = r.json()
            content = (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
            parsed = _extract_json_object(content) or {}

            subject = str(parsed.get("subject") or "").strip()
            body = str(parsed.get("body") or "").strip()
            facts = parsed.get("personalization_facts") or []
            if not isinstance(facts, list):
                facts = []
            facts = [str(x).strip() for x in facts if str(x).strip()][:20]
            safety = bool(parsed.get("safety_checks_passed"))

            if not body:
                last_err = "empty_body"
                break

            return {
                "ok": True,
                "subject": subject,
                "body": body,
                "personalization_facts": facts,
                "safety_checks_passed": safety,
                "raw_llm_json": content[:50_000],
            }

    except Exception as e:
        last_err = f"{type(e).__name__}: {str(e)[:300]}"

    return {"ok": False, "error": last_err or "openai_failed", "raw_llm_json": f"error: {last_err or 'openai_failed'}"}


def _tier_from_total(total: int) -> str:
    if total >= 80:
        return "A"
    if total >= 65:
        return "B"
    if total >= 50:
        return "C"
    return "D"


def _score_stub(lead: Dict[str, Any], criteria: Dict[str, Any]) -> Tuple[int, str, List[str], Dict[str, Any]]:
    """Deterministic stub: location>size>industry. Replace with real scorer later."""
    seed = lead.get("seed", {})
    business = lead.get("business", {})
    city = seed.get("city")
    state = seed.get("state")

    # Location (0-50)
    loc_points = 0
    target_loc = (criteria.get("locations_text") or "").lower()
    if state and state.lower() in target_loc:
        loc_points = 50
    elif state:
        loc_points = 25

    # Size (0-30) - placeholder: prefer 10-200 employees unless criteria parse exists
    emp = int(business.get("employee_count") or 0)
    size_points = 0
    if 10 <= emp <= 200:
        size_points = 30
    elif 1 <= emp <= 500:
        size_points = 18

    # Industry (0-20) - placeholder keyword match
    desc = (seed.get("business_description") or "").lower()
    ind_text = (criteria.get("industry_preferences_text") or "").lower()
    industry_points = 0
    if ind_text:
        # very rough: any token match
        tokens = [t.strip() for t in ind_text.replace(",", " ").split() if len(t.strip()) > 3]
        if any(t in desc for t in tokens[:10]):
            industry_points = 20
        else:
            industry_points = 8

    rules_total = min(100, loc_points + size_points + industry_points)
    tier = _tier_from_total(rules_total)
    reasons = [
        f"Location points={loc_points} (seed {city},{state}).",
        f"Size points={size_points} (Employees={emp}).",
        f"Industry points={industry_points} (seed description vs preferences).",
    ]
    components = {
        "location_fit": {"points": loc_points, "max": 50, "why": "Seed state vs campaign locations_text"},
        "size_fit": {"points": size_points, "max": 30, "why": "Employees vs target band"},
        "industry_fit": {"points": industry_points, "max": 20, "why": "Seed description vs preferences"},
    }
    return rules_total, tier, reasons, components


def _generate_tasks_stub(campaign_run_id: str, lead_id: str, lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Simple slow-burn skeleton; actual templates come from your play generator.
    owner = lead.get("owner", {})
    business = lead.get("business", {})
    email = ((owner.get("emails") or [{}])[0]).get("email")

    base_instructions = f"Send to {email}. Advisor, capital-backed mandate, sale-only."

    return [
        {
            "type": "email_1",
            "channel": "email",
            "status": "scheduled",
            "due_at_est": "2026-02-05T12:10:00Z",
            "instructions": base_instructions,
            "template_id": "E1_MANDATE_SALEONLY",
        },
        {
            "type": "call_1",
            "channel": "phone",
            "status": "scheduled",
            "due_at_est": "2026-02-05T12:35:00Z",
            "instructions": "Call main line; verify owner; route ownership-level inquiry.",
            "template_id": "CALL_VERIFY",
        },
    ]


def _message_drafts_for_lead(lead_id: str) -> List[Dict[str, Any]]:
    conn = db()
    rows = conn.execute(
        "select * from message_drafts where lead_id=? order by created_at asc",
        (lead_id,),
    ).fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "campaign_run_id": r["campaign_run_id"],
                "lead_id": r["lead_id"],
                "channel": r["channel"],
                "template_id": r["template_id"],
                "subject": r["subject"],
                "body": r["body"],
                "personalization_facts": json.loads(r["personalization_facts_json"] or "[]"),
                "safety_checks_passed": bool(r["safety_checks_passed"]),
                "created_at": r["created_at"],
            }
        )
    return out


def _extract_website_sample_from_lead(lead: Dict[str, Any], max_len: int = 2500) -> str:
    try:
        sources = ((lead.get("evidence") or {}).get("sources") or [])
        for s in sources:
            if (s or {}).get("source_type") == "website_page":
                snippets = (s or {}).get("snippets") or []
                if snippets and isinstance(snippets, list):
                    q = (snippets[0] or {}).get("quote")
                    if q:
                        return str(q)[:max_len]
        return ""
    except Exception:
        return ""


def _create_message_draft(
    *,
    campaign_run_id: str,
    lead_id: str,
    channel: str,
    template_id: str,
    subject: str,
    body: str,
    personalization_facts: List[str],
    safety_checks_passed: bool,
) -> Dict[str, Any]:
    md_id = f"md_{uuid.uuid4().hex}"
    conn = db()
    conn.execute(
        "insert into message_drafts (id,campaign_run_id,lead_id,channel,template_id,subject,body,personalization_facts_json,safety_checks_passed,created_at) values (?,?,?,?,?,?,?,?,?,?)",
        (
            md_id,
            campaign_run_id,
            lead_id,
            channel,
            template_id,
            subject,
            body,
            json.dumps(personalization_facts or []),
            1 if safety_checks_passed else 0,
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()
    return {
        "id": md_id,
        "campaign_run_id": campaign_run_id,
        "lead_id": lead_id,
        "channel": channel,
        "template_id": template_id,
        "subject": subject,
        "body": body,
        "personalization_facts": personalization_facts or [],
        "safety_checks_passed": bool(safety_checks_passed),
        "created_at": now_iso(),
    }


def _runner(campaign_run_id: str):
    cr = get_campaign_run(campaign_run_id)
    criteria = cr["criteria"]

    update_campaign_run(campaign_run_id, status="running")
    emit_event(
        campaign_run_id,
        "progress",
        "Run started",
        {
            "status": "running",
            "places_enabled": bool(GOOGLE_MAPS_API_KEY),
            "places_cache_ttl_days": 30,
        },
    )
    if not GOOGLE_MAPS_API_KEY:
        emit_event(
            campaign_run_id,
            "progress",
            "Google Places disabled (missing GOOGLE_MAPS_API_KEY)",
            {"places_enabled": False},
        )

    conn = db()
    leads = conn.execute("select id, lead_json from leads where campaign_run_id=?", (campaign_run_id,)).fetchall()
    conn.close()

    # Reset progress for this attempt so counters don't inflate across reruns.
    # (We still do not delete leads/tasks/events here; use a fresh campaign run for perfectly clean state.)
    prev = cr.get("progress") or {}
    prog = {
        "seed_rows_total": int(prev.get("seed_rows_total") or 0),
        "leads_created": int(prev.get("leads_created") or 0),
        "leads_enriched_places": 0,
        "leads_enriched_website": 0,
        "leads_scored": 0,
        "tasks_created": 0,
        "tasks_completed": int(prev.get("tasks_completed") or 0),
        "errors": 0,
    }
    update_campaign_run(campaign_run_id, progress=prog)
    emit_event(campaign_run_id, "progress", "Progress reset for run attempt", {"progress": prog})

    for row in leads:
        # Allow abort from API.
        if get_campaign_run_status(campaign_run_id) == "aborted":
            emit_event(campaign_run_id, "progress", "Run aborted", {"status": "aborted", "progress": prog})
            return

        lead_id = row["id"]
        lead = json.loads(row["lead_json"])

        try:
            # Places enrich
            seed = lead.get("seed", {})
            places = _places_lookup(
                seed.get("business_name", ""),
                seed.get("city", ""),
                seed.get("state", ""),
                website_hint=seed.get("website", ""),
            )
            if places:
                lead["business"].setdefault("google", {})
                lead["business"]["google"] = {
                    "place_id": places.get("place_id"),
                    "maps_url": places.get("url"),
                    "rating": places.get("rating"),
                    "reviews": places.get("user_ratings_total"),
                    "types": places.get("types"),
                    "phone": places.get("formatted_phone_number"),
                    "website": places.get("website"),
                    "formatted_address": places.get("formatted_address"),
                    "latlng": ((places.get("geometry") or {}).get("location") or {}),
                    "hours_local": (places.get("opening_hours") or {}).get("weekday_text"),
                }
                lead["evidence"]["sources"].append(
                    {
                        "source_type": "google_places",
                        "url": places.get("url"),
                        "captured_at": now_iso(),
                        "fields": {
                            "formatted_address": places.get("formatted_address"),
                            "formatted_phone_number": places.get("formatted_phone_number"),
                            "rating": places.get("rating"),
                            "user_ratings_total": places.get("user_ratings_total"),
                            "website": places.get("website"),
                        },
                    }
                )
                prog["leads_enriched_places"] = min(
                    prog["leads_enriched_places"] + 1,
                    prog.get("seed_rows_total") or prog["leads_enriched_places"] + 1,
                )
                emit_event(
                    campaign_run_id,
                    "lead.places_enriched",
                    "Places enriched",
                    {"rating": places.get("rating"), "reviews": places.get("user_ratings_total"), "maps_url": places.get("url")},
                    lead_id=lead_id,
                )

            # Website enrich (POC: fetch homepage only)
            html = _website_fetch(lead.get("seed", {}).get("website") or "")
            text = _strip_html(html)
            if text:
                lead["evidence"]["sources"].append(
                    {
                        "source_type": "website_page",
                        "url": lead.get("seed", {}).get("website"),
                        "captured_at": now_iso(),
                        "snippets": [{"quote": text[:300], "label": "homepage_text_sample"}],
                    }
                )
                prog["leads_enriched_website"] = min(
                    prog["leads_enriched_website"] + 1,
                    prog.get("seed_rows_total") or prog["leads_enriched_website"] + 1,
                )
                emit_event(
                    campaign_run_id,
                    "lead.website_enriched",
                    "Website enriched",
                    {"sample_len": len(text)},
                    lead_id=lead_id,
                )

            # Extract BusinessProfile (AI) after enrichment
            try:
                evidence_used: List[str] = []
                seed_desc = (seed.get("business_description") or "").strip()
                if seed_desc:
                    evidence_used.append("seed_description")
                if places:
                    evidence_used.append("google_places")
                if text:
                    evidence_used.append("website_page")

                evidence_payload = {
                    "evidence_used": evidence_used,
                    "seed": {
                        "business_name": seed.get("business_name"),
                        "city": seed.get("city"),
                        "state": seed.get("state"),
                        "business_description": seed_desc[:2000],
                    },
                    "google_places": (lead.get("business") or {}).get("google"),
                    "website_sample": (text[:2500] if text else ""),
                }

                bp_core = _llm_business_profile(evidence_payload)
                bp_saved = upsert_business_profile(lead_id, bp_core)
                lead["business_profile"] = bp_saved

                emit_event(
                    campaign_run_id,
                    "lead.business_profile_extracted",
                    "BusinessProfile extracted",
                    {"vertical_category": bp_saved.get("vertical_category"), "confidence": bp_saved.get("confidence")},
                    lead_id=lead_id,
                )
            except Exception as e:
                prog["errors"] = int(prog.get("errors") or 0) + 1
                bp_saved = upsert_business_profile(lead_id, _bp_default([]))
                lead["business_profile"] = bp_saved
                emit_event(
                    campaign_run_id,
                    "lead.business_profile_error",
                    "BusinessProfile extraction error; defaulted",
                    {"error": f"{type(e).__name__}: {str(e)[:200]}"},
                    lead_id=lead_id,
                )

            # Score (rules + AI hybrid)
            rules_total, _rules_tier, rules_reasons, components = _score_stub(lead, criteria)

            # Build evidence payload for AI scoring (grounded)
            seed = lead.get("seed", {})
            ai_payload = {
                "criteria": criteria,
                "rules_score": {"rules_total": rules_total, "components": components},
                "business_profile": lead.get("business_profile") or {},
                "seed": {
                    "business_name": seed.get("business_name"),
                    "city": seed.get("city"),
                    "state": seed.get("state"),
                    "employees": seed.get("employees"),
                    "business_description": (seed.get("business_description") or "")[:2000],
                    "website": seed.get("website"),
                },
                "places": (lead.get("business") or {}).get("google") or {},
                "website_sample": ((text[:2500] if text else "") or ""),
            }

            ai = _llm_ai_score_lead(ai_payload)
            ai_used = bool(ai.get("ai_used"))
            ai_total = ai.get("ai_total")

            if ai_used and isinstance(ai_total, int):
                # Clamp to keep demo stable and prevent hallucinated swings
                final_total = int(max(rules_total - 25, min(rules_total + 25, ai_total)))
                final_tier = _tier_from_total(final_total)
                reasoning_bullets = (ai.get("reasoning_bullets") or [])[:4]
                reasoning_citations = ai.get("reasoning_citations") or []
                emit_event(
                    campaign_run_id,
                    "lead.ai_scored",
                    "AI score computed",
                    {"ai_total": ai_total, "final_total": final_total, "ai_used": True},
                    lead_id=lead_id,
                )
            else:
                final_total = int(rules_total)
                final_tier = _tier_from_total(final_total)
                ai_total = int(rules_total)
                reasoning_bullets = rules_reasons[:4]
                reasoning_citations = []
                emit_event(
                    campaign_run_id,
                    "lead.ai_scored_error",
                    "AI scoring failed; defaulted to rules score",
                    {"ai_used": False, "error": (ai.get("raw_llm_json") or "")[:200]},
                    lead_id=lead_id,
                )

            # Back-compat: score.total is the sortable total. We'll store FINAL there.
            lead["score"] = {
                "total": final_total,
                "rules_total": rules_total,
                "ai_total": ai_total,
                "final_total": final_total,
                "tier": final_tier,
                "ai_used": bool(ai_used),
                "components": components,
                "reasoning_bullets": reasoning_bullets,
                "reasoning_citations": reasoning_citations,
            }

            prog["leads_scored"] = min(prog["leads_scored"] + 1, prog.get("seed_rows_total") or prog["leads_scored"] + 1)
            emit_event(campaign_run_id, "lead.scored", "Lead scored", {"rules_total": rules_total, "final_total": final_total, "tier": final_tier}, lead_id=lead_id)

            # for downstream
            total = final_total
            tier = final_tier

            # Persist lead (even if later stages fail)
            conn = db()
            conn.execute(
                "update leads set lead_json=?, score_total=?, tier=?, updated_at=? where id=?",
                (json.dumps(lead), total, tier, now_iso(), lead_id),
            )
            conn.commit()
            conn.close()

            update_campaign_run(campaign_run_id, progress=prog)

        except Exception as e:
            prog["errors"] = int(prog.get("errors") or 0) + 1
            emit_event(
                campaign_run_id,
                "lead.error",
                "Lead processing error (skipped)",
                {"error": f"{type(e).__name__}: {str(e)[:300]}"},
                lead_id=lead_id,
            )
            update_campaign_run(campaign_run_id, progress=prog)
            continue

        # MessageDrafts + Tasks for A/B
        if tier in ("A", "B"):
            # Experiment allocation (campaign-level activation, lead-level assignment)
            exp = _active_experiment_for_campaign(campaign_run_id)
            tactic_id = None
            experiment_id = None
            email_template_id = "E1_MANDATE_SALEONLY"
            if exp and exp.get("status") == "active" and exp.get("tactic"):
                seg = (exp.get("tactic") or {}).get("target_segment") or {}
                if _lead_matches_segment(lead, seg):
                    pct = int(exp.get("allocation_pct") or 0)
                    if _stable_bucket_0_99(lead_id) < max(0, min(100, pct)):
                        tactic_id = (exp.get("tactic") or {}).get("id")
                        experiment_id = exp.get("id")
                        pat = (exp.get("tactic") or {}).get("pattern") or {}
                        if isinstance(pat, dict) and pat.get("email_template_id"):
                            email_template_id = str(pat.get("email_template_id"))

            # Generate at least 1 email draft (idempotent-ish)
            try:
                conn = db()
                md_existing = conn.execute(
                    "select count(1) as c from message_drafts where campaign_run_id=? and lead_id=? and channel='email'",
                    (campaign_run_id, lead_id),
                ).fetchone()["c"]
                conn.close()

                if not md_existing or int(md_existing) == 0:
                    seed = lead.get("seed", {})
                    md_payload = {
                        "channel": "email",
                        "template_id": email_template_id,
                        "criteria": criteria,
                        "business_profile": lead.get("business_profile") or {},
                        "seed": {
                            "business_name": seed.get("business_name"),
                            "city": seed.get("city"),
                            "state": seed.get("state"),
                            "employees": seed.get("employees"),
                            "business_description": (seed.get("business_description") or "")[:2000],
                            "website": seed.get("website"),
                        },
                        "places": (lead.get("business") or {}).get("google") or {},
                        "website_sample": ((text[:2500] if text else "") or ""),
                    }

                    md = _llm_message_draft(md_payload)
                    if md.get("ok"):
                        saved = _create_message_draft(
                            campaign_run_id=campaign_run_id,
                            lead_id=lead_id,
                            channel="email",
                            template_id=email_template_id,
                            subject=str(md.get("subject") or "").strip(),
                            body=str(md.get("body") or "").strip(),
                            personalization_facts=[str(x) for x in (md.get("personalization_facts") or [])],
                            safety_checks_passed=bool(md.get("safety_checks_passed")),
                        )
                        emit_event(
                            campaign_run_id,
                            "lead.message_draft_created",
                            "Message draft created",
                            {"draft_id": saved.get("id"), "channel": "email", "template_id": email_template_id, "safety_checks_passed": saved.get("safety_checks_passed"), "tactic_id": tactic_id, "experiment_id": experiment_id},
                            lead_id=lead_id,
                        )
                    else:
                        emit_event(
                            campaign_run_id,
                            "lead.message_draft_error",
                            "Message draft failed (skipped)",
                            {"error": (md.get("error") or md.get("raw_llm_json") or "")[:200]},
                            lead_id=lead_id,
                        )
            except Exception as e:
                emit_event(
                    campaign_run_id,
                    "lead.message_draft_error",
                    "Message draft error (skipped)",
                    {"error": f"{type(e).__name__}: {str(e)[:200]}"},
                    lead_id=lead_id,
                )

            # Avoid duplicating tasks on force reruns
            conn = db()
            existing = conn.execute(
                "select count(1) as c from tasks where campaign_run_id=? and lead_id=?",
                (campaign_run_id, lead_id),
            ).fetchone()["c"]
            conn.close()
            if not existing or int(existing) == 0:
                tasks = _generate_tasks_stub(campaign_run_id, lead_id, lead)
                task_ids = []
                conn = db()
                for t in tasks:
                    tid = f"tsk_{uuid.uuid4().hex}"
                    task_ids.append(tid)
                    tpl = t.get("template_id")
                    if str(t.get("channel")) == "email":
                        tpl = email_template_id
                    conn.execute(
                        "insert into tasks (id,campaign_run_id,lead_id,type,channel,status,due_at_est,window_start_est,window_end_est,instructions,template_id,tactic_id,experiment_id,completion_json,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            tid,
                            campaign_run_id,
                            lead_id,
                            t["type"],
                            t["channel"],
                            t["status"],
                            t.get("due_at_est"),
                            t.get("window_start_est"),
                            t.get("window_end_est"),
                            t.get("instructions"),
                            tpl,
                            tactic_id,
                            experiment_id,
                            json.dumps({"completed_at": None, "outcome_code": None, "outcome_notes": None}),
                            now_iso(),
                            now_iso(),
                        ),
                    )
                conn.commit()
                conn.close()

                lead["task_ids"] = task_ids
                prog["tasks_created"] += len(task_ids)
                emit_event(
                    campaign_run_id,
                    "lead.tasks_created",
                    "Tasks created",
                    {"task_count": len(task_ids)},
                    lead_id=lead_id,
                )

        # Persist lead
        conn = db()
        conn.execute(
            "update leads set lead_json=?, score_total=?, tier=?, updated_at=? where id=?",
            (json.dumps(lead), total, tier, now_iso(), lead_id),
        )
        conn.commit()
        conn.close()

        update_campaign_run(campaign_run_id, progress=prog)

    # If no A/B tasks were created, create tasks for top leads anyway (POC default)
    if prog.get("tasks_created", 0) == 0:
        top_n = int(os.environ.get("TASK_TOP_N", "25"))
        conn = db()
        top_rows = conn.execute(
            "select id, lead_json from leads where campaign_run_id=? order by coalesce(score_total, -1) desc, id asc limit ?",
            (campaign_run_id, top_n),
        ).fetchall()
        conn.close()

        for r in top_rows:
            if get_campaign_run_status(campaign_run_id) == "aborted":
                emit_event(campaign_run_id, "progress", "Run aborted", {"status": "aborted", "progress": prog})
                return

            lead_id = r["id"]
            lead = json.loads(r["lead_json"])

            # Generate at least 1 email draft for fallback top N (idempotent-ish)
            try:
                conn = db()
                md_existing = conn.execute(
                    "select count(1) as c from message_drafts where campaign_run_id=? and lead_id=? and channel='email'",
                    (campaign_run_id, lead_id),
                ).fetchone()["c"]
                conn.close()

                if not md_existing or int(md_existing) == 0:
                    seed = lead.get("seed", {})
                    md_payload = {
                        "channel": "email",
                        "template_id": "E1_MANDATE_SALEONLY",
                        "criteria": criteria,
                        "business_profile": lead.get("business_profile") or {},
                        "seed": {
                            "business_name": seed.get("business_name"),
                            "city": seed.get("city"),
                            "state": seed.get("state"),
                            "employees": seed.get("employees"),
                            "business_description": (seed.get("business_description") or "")[:2000],
                            "website": seed.get("website"),
                        },
                        "places": (lead.get("business") or {}).get("google") or {},
                        "website_sample": "",
                    }
                    md = _llm_message_draft(md_payload)
                    if md.get("ok"):
                        saved = _create_message_draft(
                            campaign_run_id=campaign_run_id,
                            lead_id=lead_id,
                            channel="email",
                            template_id="E1_MANDATE_SALEONLY",
                            subject=str(md.get("subject") or "").strip(),
                            body=str(md.get("body") or "").strip(),
                            personalization_facts=[str(x) for x in (md.get("personalization_facts") or [])],
                            safety_checks_passed=bool(md.get("safety_checks_passed")),
                        )
                        emit_event(
                            campaign_run_id,
                            "lead.message_draft_created",
                            "Message draft created",
                            {"draft_id": saved.get("id"), "channel": "email", "template_id": "E1_MANDATE_SALEONLY", "reason": "fallback_top_n", "safety_checks_passed": saved.get("safety_checks_passed")},
                            lead_id=lead_id,
                        )
            except Exception:
                pass

            # skip if tasks already exist
            conn = db()
            existing = conn.execute("select count(1) as c from tasks where lead_id=?", (lead_id,)).fetchone()["c"]
            conn.close()
            if existing and int(existing) > 0:
                continue

            tasks = _generate_tasks_stub(campaign_run_id, lead_id, lead)
            task_ids: List[str] = []
            conn = db()
            for t in tasks:
                tid = f"tsk_{uuid.uuid4().hex}"
                task_ids.append(tid)
                conn.execute(
                    "insert into tasks (id,campaign_run_id,lead_id,type,channel,status,due_at_est,window_start_est,window_end_est,instructions,template_id,completion_json,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        tid,
                        campaign_run_id,
                        lead_id,
                        t["type"],
                        t["channel"],
                        t["status"],
                        t.get("due_at_est"),
                        t.get("window_start_est"),
                        t.get("window_end_est"),
                        t.get("instructions"),
                        t.get("template_id"),
                        json.dumps({"completed_at": None, "outcome_code": None, "outcome_notes": None}),
                        now_iso(),
                        now_iso(),
                    ),
                )
            conn.commit()
            conn.close()

            lead["task_ids"] = task_ids
            conn = db()
            conn.execute("update leads set lead_json=?, updated_at=? where id=?", (json.dumps(lead), now_iso(), lead_id))
            conn.commit()
            conn.close()

            prog["tasks_created"] += len(task_ids)
            emit_event(
                campaign_run_id,
                "lead.tasks_created",
                "Tasks created",
                {"task_count": len(task_ids), "reason": "fallback_top_n"},
                lead_id=lead_id,
            )

        update_campaign_run(campaign_run_id, progress=prog)

    if get_campaign_run_status(campaign_run_id) == "aborted":
        emit_event(campaign_run_id, "progress", "Run aborted", {"status": "aborted", "progress": prog})
        return

    update_campaign_run(campaign_run_id, status="complete", progress=prog)
    emit_event(campaign_run_id, "progress", "Run complete", {"status": "complete", "progress": prog})


@app.post("/v1/campaign-runs/{campaign_run_id}/run", dependencies=[Depends(auth)])
def run_campaign(campaign_run_id: str, force: bool = False):
    cr = get_campaign_run(campaign_run_id)

    # Prevent concurrent runs (these inflate counters + duplicate events)
    if cr.get("status") == "running":
        raise HTTPException(status_code=409, detail="Campaign run is already running")

    # Avoid accidental re-runs of completed jobs unless explicitly forced
    if cr.get("status") == "complete" and not force:
        raise HTTPException(status_code=409, detail="Campaign run already complete (pass force=true to rerun)")

    # fire-and-forget thread
    t = threading.Thread(target=_runner, args=(campaign_run_id,), daemon=True)
    t.start()
    return get_campaign_run(campaign_run_id)


@app.get("/v1/campaign-runs/{campaign_run_id}/events", dependencies=[Depends(auth)])
def poll_events(campaign_run_id: str, cursor: Optional[str] = None, limit: int = 200):
    conn = db()
    q = "select * from events where campaign_run_id=? order by time asc"
    rows = conn.execute(q, (campaign_run_id,)).fetchall()
    conn.close()

    # naive cursor: event id
    start = 0
    if cursor:
        for i, r in enumerate(rows):
            if r["id"] == cursor:
                start = i + 1
                break
    sliced = rows[start : start + limit]
    events = []
    for r in sliced:
        events.append(
            {
                "id": r["id"],
                "campaign_run_id": r["campaign_run_id"],
                "lead_id": r["lead_id"],
                "time": r["time"],
                "type": r["type"],
                "message": r["message"],
                "payload": json.loads(r["payload_json"] or "{}"),
            }
        )
    next_cursor = sliced[-1]["id"] if sliced else None
    return {"events": events, "nextCursor": next_cursor}


@app.get("/v1/campaign-runs/{campaign_run_id}/events/stream")
def stream_events(campaign_run_id: str, creds: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    # SSE auth manually
    if not creds or creds.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    async def gen():
        last_id = None
        while True:
            conn = db()
            rows = conn.execute(
                "select * from events where campaign_run_id=? order by time asc", (campaign_run_id,)
            ).fetchall()
            conn.close()
            for r in rows:
                if last_id and r["id"] == last_id:
                    continue
            # emit only new
            to_emit = []
            if last_id:
                seen = False
                for r in rows:
                    if seen:
                        to_emit.append(r)
                    if r["id"] == last_id:
                        seen = True
            else:
                to_emit = rows[-50:]
            for r in to_emit:
                last_id = r["id"]
                yield {
                    "event": r["type"],
                    "id": r["id"],
                    "data": json.dumps(
                        {
                            "id": r["id"],
                            "campaign_run_id": r["campaign_run_id"],
                            "lead_id": r["lead_id"],
                            "time": r["time"],
                            "type": r["type"],
                            "message": r["message"],
                            "payload": json.loads(r["payload_json"] or "{}"),
                        }
                    ),
                }
            await asyncio.sleep(1)

    import asyncio

    return EventSourceResponse(gen())


@app.get("/v1/campaign-runs/{campaign_run_id}/leads", dependencies=[Depends(auth)])
def list_leads(campaign_run_id: str, tier: Optional[str] = None, sort: str = "score_desc", limit: int = 50, cursor: Optional[str] = None):
    tiers = [t.strip() for t in (tier or "").split(",") if t.strip()]

    conn = db()
    sql = "select id, lead_json, score_total, tier from leads where campaign_run_id=?"
    params: List[Any] = [campaign_run_id]
    if tiers:
        sql += " and tier in (%s)" % ",".join(["?"] * len(tiers))
        params.extend(tiers)
    order = "desc" if sort == "score_desc" else "asc"
    sql += f" order by coalesce(score_total, -1) {order}, id asc"
    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()

    # cursor is lead id
    start = 0
    if cursor:
        for i, r in enumerate(rows):
            if r["id"] == cursor:
                start = i + 1
                break
    sliced = rows[start : start + limit]

    leads_out = []
    for r in sliced:
        lj = json.loads(r["lead_json"])
        reasons = (lj.get("score") or {}).get("reasoning_bullets") or []
        next_task = None
        # find earliest scheduled task
        conn = db()
        tr = conn.execute(
            "select id, type, due_at_est from tasks where lead_id=? and status in ('scheduled','todo') order by due_at_est asc limit 1",
            (r["id"],),
        ).fetchone()
        conn.close()
        if tr:
            next_task = {"task_id": tr["id"], "type": tr["type"], "due_at_est": tr["due_at_est"]}

        bp = lj.get("business_profile") or {}
        leads_out.append(
            {
                "id": r["id"],
                "owner_name": (lj.get("owner") or {}).get("full_name"),
                "owner_email": (((lj.get("owner") or {}).get("emails") or [{}])[0]).get("email"),
                "business_name": (lj.get("business") or {}).get("name"),
                "city": ((lj.get("business") or {}).get("address") or {}).get("city"),
                "state": ((lj.get("business") or {}).get("address") or {}).get("state"),
                "employees": (lj.get("business") or {}).get("employee_count"),
                "score_total": r["score_total"],
                "tier": r["tier"],
                # BusinessProfile summary fields for Lovable list UI
                "vertical_category": bp.get("vertical_category"),
                "profile_confidence": bp.get("confidence"),
                "top_reasons": reasons[:3],
                "next_task": next_task,
            }
        )

    next_cursor = sliced[-1]["id"] if sliced else None
    return {"leads": leads_out, "nextCursor": next_cursor}


@app.get("/v1/leads/{lead_id}", dependencies=[Depends(auth)])
def lead_detail(lead_id: str):
    conn = db()
    row = conn.execute("select lead_json from leads where id=?", (lead_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Lead not found")
    lead = json.loads(row["lead_json"])
    lead["message_drafts"] = _message_drafts_for_lead(lead_id)
    return lead


@app.get("/v1/message-drafts", dependencies=[Depends(auth)])
def list_message_drafts(campaign_run_id: str, lead_id: Optional[str] = None, channel: Optional[str] = None, limit: int = 200):
    limit = max(1, min(int(limit or 200), 500))
    conn = db()
    sql = "select * from message_drafts where campaign_run_id=?"
    params: List[Any] = [campaign_run_id]
    if lead_id:
        sql += " and lead_id=?"
        params.append(lead_id)
    if channel:
        sql += " and channel=?"
        params.append(channel)
    sql += " order by created_at asc limit ?"
    params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()

    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append(
            {
                "id": r["id"],
                "campaign_run_id": r["campaign_run_id"],
                "lead_id": r["lead_id"],
                "channel": r["channel"],
                "template_id": r["template_id"],
                "subject": r["subject"],
                "body": r["body"],
                "personalization_facts": json.loads(r["personalization_facts_json"] or "[]"),
                "safety_checks_passed": bool(r["safety_checks_passed"]),
                "created_at": r["created_at"],
            }
        )

    return {"items": items}


@app.post("/v1/campaign-runs/{campaign_run_id}/generate-message-drafts", dependencies=[Depends(auth)])
def generate_message_drafts(campaign_run_id: str, tier: str = "A,B", limit: int = 25):
    """Generate MessageDrafts after-the-fact (escape hatch).

    Useful if a run completed before drafts existed, or if drafts were disabled.
    Generates 1 email draft per matching lead if missing.
    """
    tiers = [t.strip() for t in (tier or "").split(",") if t.strip()]
    limit = max(1, min(int(limit or 25), 200))

    # ensure run exists
    _ = get_campaign_run(campaign_run_id)

    conn = db()
    sql = "select id, lead_json from leads where campaign_run_id=?"
    params: List[Any] = [campaign_run_id]
    if tiers:
        sql += " and tier in (%s)" % ",".join(["?"] * len(tiers))
        params.extend(tiers)
    sql += " order by coalesce(score_total, -1) desc, id asc limit ?"
    params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()

    created = 0
    skipped = 0
    for r in rows:
        lead_id = r["id"]
        lead = json.loads(r["lead_json"])

        conn = db()
        md_existing = conn.execute(
            "select count(1) as c from message_drafts where campaign_run_id=? and lead_id=? and channel='email'",
            (campaign_run_id, lead_id),
        ).fetchone()["c"]
        conn.close()
        if md_existing and int(md_existing) > 0:
            skipped += 1
            continue

        seed = lead.get("seed", {})
        website_sample = _extract_website_sample_from_lead(lead)
        md_payload = {
            "channel": "email",
            "template_id": "E1_MANDATE_SALEONLY",
            "criteria": (get_campaign_run(campaign_run_id).get("criteria") or {}),
            "business_profile": lead.get("business_profile") or {},
            "seed": {
                "business_name": seed.get("business_name"),
                "city": seed.get("city"),
                "state": seed.get("state"),
                "employees": seed.get("employees"),
                "business_description": (seed.get("business_description") or "")[:2000],
                "website": seed.get("website"),
            },
            "places": (lead.get("business") or {}).get("google") or {},
            "website_sample": website_sample,
        }

        md = _llm_message_draft(md_payload)
        if not md.get("ok"):
            emit_event(
                campaign_run_id,
                "lead.message_draft_error",
                "Message draft failed (skipped)",
                {"error": (md.get("error") or md.get("raw_llm_json") or "")[:200]},
                lead_id=lead_id,
            )
            continue

        saved = _create_message_draft(
            campaign_run_id=campaign_run_id,
            lead_id=lead_id,
            channel="email",
            template_id="E1_MANDATE_SALEONLY",
            subject=str(md.get("subject") or "").strip(),
            body=str(md.get("body") or "").strip(),
            personalization_facts=[str(x) for x in (md.get("personalization_facts") or [])],
            safety_checks_passed=bool(md.get("safety_checks_passed")),
        )
        created += 1
        emit_event(
            campaign_run_id,
            "lead.message_draft_created",
            "Message draft created",
            {"draft_id": saved.get("id"), "channel": "email", "template_id": "E1_MANDATE_SALEONLY", "reason": "manual_generate", "safety_checks_passed": saved.get("safety_checks_passed")},
            lead_id=lead_id,
        )

    return {"campaign_run_id": campaign_run_id, "created": created, "skipped": skipped, "considered": len(rows)}


# --- Tactics + Experiments (Campaign intelligence loop) ---

_ALLOWED_TACTIC_CHANNELS = {"email", "phone"}
_DENYLIST_TACTIC_TERMS = [
    "scrape",
    "scraping",
    "pretend",
    "impersonate",
    "deceptive",
    "facebook",
    "instagram",
    "tiktok",
    "personal social",
    "dox",
]


def _stable_bucket_0_99(key: str) -> int:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % 100


def _active_experiment_for_campaign(campaign_run_id: str) -> Optional[Dict[str, Any]]:
    conn = db()
    row = conn.execute(
        "select * from experiments where campaign_run_id=? and status='active' order by started_at desc limit 1",
        (campaign_run_id,),
    ).fetchone()
    conn.close()
    if not row:
        return None
    exp = dict(row)
    # attach tactic
    conn = db()
    tr = conn.execute("select * from tactics where id=?", (row["tactic_id"],)).fetchone()
    conn.close()
    tactic = dict(tr) if tr else None
    if tactic:
        exp["tactic"] = {
            "id": tactic.get("id"),
            "name": tactic.get("name"),
            "description": tactic.get("description"),
            "channels": json.loads(tactic.get("channels_json") or "[]"),
            "target_segment": json.loads(tactic.get("target_segment_json") or "{}"),
            "hypothesis": tactic.get("hypothesis"),
            "pattern": json.loads(tactic.get("pattern_json") or "{}"),
            "status": tactic.get("status"),
        }
    return exp


def _lead_matches_segment(lead: Dict[str, Any], segment: Dict[str, Any]) -> bool:
    if not segment:
        return True
    score = lead.get("score") or {}
    bp = lead.get("business_profile") or {}
    tier = score.get("tier")
    vc = bp.get("vertical_category")

    if "tier" in segment:
        allowed = segment.get("tier") or []
        if isinstance(allowed, str):
            allowed = [allowed]
        if allowed and tier not in allowed:
            return False

    if "vertical_category" in segment:
        allowed = segment.get("vertical_category") or []
        if isinstance(allowed, str):
            allowed = [allowed]
        if allowed and vc not in allowed:
            return False

    return True


def _sanitize_tactic_proposal(t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name = str(t.get("name") or "").strip()[:120]
    desc = str(t.get("description") or "").strip()[:800]
    hypothesis = str(t.get("hypothesis") or "").strip()[:800]

    blob = f"{name} {desc} {hypothesis}".lower()
    if any(term in blob for term in _DENYLIST_TACTIC_TERMS):
        return None

    channels = t.get("channels") or []
    if isinstance(channels, str):
        channels = [channels]
    channels = [str(c).strip() for c in channels if str(c).strip()]
    channels = [c for c in channels if c in _ALLOWED_TACTIC_CHANNELS]
    if not channels:
        channels = ["email"]

    segment = t.get("target_segment") or {}
    if not isinstance(segment, dict):
        segment = {}

    pattern = t.get("pattern") or {}
    if not isinstance(pattern, dict):
        pattern = {}

    return {
        "name": name or "Tactic",
        "description": desc or "",
        "hypothesis": hypothesis or "",
        "channels": channels,
        "target_segment": segment,
        "pattern": pattern,
    }


def _llm_generate_tactics(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        return []

    system = (
        "You propose outreach tactics for a B2B campaign. "
        "Return STRICT JSON only. Do not propose spammy or deceptive tactics. "
        "Only use allowed channels: email, phone."
    )

    user = {
        "task": "Propose 3-5 new outreach tactics",
        "allowed_channels": ["email", "phone"],
        "schema": {
            "tactics": [
                {
                    "name": "string",
                    "description": "string",
                    "channels": "string[]",
                    "target_segment": "object (e.g. {tier:[A], vertical_category:[pest_control]})",
                    "hypothesis": "string",
                    "pattern": "object (e.g. {email_template_id: string, angle: string})",
                }
            ]
        },
        "input": payload,
    }

    last_err: Optional[str] = None
    for attempt in range(OPENAI_MAX_RETRIES):
        _openai_throttle()
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "temperature": 0.4,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user)},
                ],
                "response_format": {"type": "json_object"},
            },
            timeout=60,
        )
        if r.status_code == 429:
            time.sleep(min(8.0, 0.75 * (2**attempt)))
            last_err = "429"
            continue
        if r.status_code >= 400:
            last_err = (r.text or "")[:200]
            break
        data = r.json()
        content = (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
        parsed = _extract_json_object(content) or {}
        tactics = parsed.get("tactics") or []
        if not isinstance(tactics, list):
            return []
        return tactics[:5]

    return []


def _create_tactic(campaign_run_id: str, t: Dict[str, Any], created_by: str = "ai", status: str = "proposed") -> Dict[str, Any]:
    tid = f"tc_{uuid.uuid4().hex}"
    now = now_iso()
    conn = db()
    conn.execute(
        "insert into tactics (id,campaign_run_id,name,description,channels_json,target_segment_json,hypothesis,pattern_json,created_by,status,created_at,updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            tid,
            campaign_run_id,
            t.get("name"),
            t.get("description"),
            json.dumps(t.get("channels") or []),
            json.dumps(t.get("target_segment") or {}),
            t.get("hypothesis"),
            json.dumps(t.get("pattern") or {}),
            created_by,
            status,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()
    return {"id": tid, "campaign_run_id": campaign_run_id, **t, "created_by": created_by, "status": status, "created_at": now, "updated_at": now}


def _list_tactics(campaign_run_id: str) -> List[Dict[str, Any]]:
    conn = db()
    rows = conn.execute("select * from tactics where campaign_run_id=? order by created_at desc", (campaign_run_id,)).fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "campaign_run_id": r["campaign_run_id"],
                "name": r["name"],
                "description": r["description"],
                "channels": json.loads(r["channels_json"] or "[]"),
                "target_segment": json.loads(r["target_segment_json"] or "{}"),
                "hypothesis": r["hypothesis"],
                "pattern": json.loads(r["pattern_json"] or "{}"),
                "created_by": r["created_by"],
                "status": r["status"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            }
        )
    return out


@app.get("/v1/tactics", dependencies=[Depends(auth)])
def list_tactics(campaign_run_id: str):
    _ = get_campaign_run(campaign_run_id)
    return {"items": _list_tactics(campaign_run_id)}


@app.post("/v1/campaign-runs/{campaign_run_id}/tactics/generate", dependencies=[Depends(auth)])
def generate_tactics_for_campaign(campaign_run_id: str):
    cr = get_campaign_run(campaign_run_id)

    # Aggregate outcomes by template + segment
    conn = db()
    attempt_rows = conn.execute(
        "select channel, template_id, outcome_code, count(1) as c from outreach_attempts where campaign_run_id=? group by channel, template_id, outcome_code",
        (campaign_run_id,),
    ).fetchall()

    # BusinessProfile distribution
    bp_rows = conn.execute(
        "select bp.vertical_category as vc, count(1) as c from business_profiles bp join leads l on l.id=bp.lead_id where l.campaign_run_id=? group by bp.vertical_category order by c desc limit 20",
        (campaign_run_id,),
    ).fetchall()
    conn.close()

    attempts_summary = [dict(r) for r in attempt_rows]
    bp_dist = [dict(r) for r in bp_rows]

    payload = {
        "campaign": {"id": campaign_run_id, "criteria": cr.get("criteria")},
        "attempts_summary": attempts_summary,
        "business_profile_distribution": bp_dist,
    }

    raw = _llm_generate_tactics(payload)
    created: List[Dict[str, Any]] = []
    for t in raw:
        st = _sanitize_tactic_proposal(t)
        if not st:
            continue
        created.append(_create_tactic(campaign_run_id, st, created_by="ai", status="proposed"))

    emit_event(campaign_run_id, "campaign.tactics_generated", "Tactics generated", {"count": len(created)})
    return {"items": created}


@app.post("/v1/tactics/{tactic_id}/activate", dependencies=[Depends(auth)])
def activate_tactic(tactic_id: str, allocation_pct: int = 10):
    allocation_pct = max(1, min(int(allocation_pct or 10), 50))
    conn = db()
    t = conn.execute("select * from tactics where id=?", (tactic_id,)).fetchone()
    if not t:
        conn.close()
        raise HTTPException(404, "Tactic not found")

    campaign_run_id = t["campaign_run_id"]

    # end any active experiment for campaign (one-at-a-time MVP)
    conn.execute(
        "update experiments set status='ended', ended_at=? where campaign_run_id=? and status='active'",
        (now_iso(), campaign_run_id),
    )

    ex_id = f"ex_{uuid.uuid4().hex}"
    conn.execute(
        "insert into experiments (id,campaign_run_id,tactic_id,allocation_pct,started_at,ended_at,metrics_snapshot_json,status) values (?,?,?,?,?,?,?,?)",
        (ex_id, campaign_run_id, tactic_id, allocation_pct, now_iso(), None, json.dumps({}), "active"),
    )
    conn.execute("update tactics set status='active', updated_at=? where id=?", (now_iso(), tactic_id))
    conn.commit()
    conn.close()

    emit_event(campaign_run_id, "campaign.experiment_started", "Experiment started", {"experiment_id": ex_id, "tactic_id": tactic_id, "allocation_pct": allocation_pct})
    return {"experiment_id": ex_id, "tactic_id": tactic_id, "allocation_pct": allocation_pct}


@app.get("/v1/experiments", dependencies=[Depends(auth)])
def list_experiments(campaign_run_id: str):
    _ = get_campaign_run(campaign_run_id)
    conn = db()
    rows = conn.execute("select * from experiments where campaign_run_id=? order by started_at desc", (campaign_run_id,)).fetchall()
    conn.close()
    return {"items": [dict(r) for r in rows]}


@app.post("/v1/experiments/{experiment_id}/end", dependencies=[Depends(auth)])
def end_experiment(experiment_id: str):
    conn = db()
    row = conn.execute("select * from experiments where id=?", (experiment_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Experiment not found")
    campaign_run_id = row["campaign_run_id"]
    conn.execute("update experiments set status='ended', ended_at=? where id=?", (now_iso(), experiment_id))
    conn.commit()
    conn.close()
    emit_event(campaign_run_id, "campaign.experiment_ended", "Experiment ended", {"experiment_id": experiment_id})
    return {"experiment_id": experiment_id, "status": "ended"}


@app.get("/v1/tasks", dependencies=[Depends(auth)])
def list_tasks(campaign_run_id: Optional[str] = None, status: Optional[str] = None, due_before: Optional[str] = None, limit: int = 200):
    statuses = [s.strip() for s in (status or "").split(",") if s.strip()]
    conn = db()
    sql = "select * from tasks where 1=1"
    params: List[Any] = []
    if campaign_run_id:
        sql += " and campaign_run_id=?"
        params.append(campaign_run_id)
    if statuses:
        sql += " and status in (%s)" % ",".join(["?"] * len(statuses))
        params.extend(statuses)
    if due_before:
        sql += " and due_at_est <= ?"
        params.append(due_before)
    sql += " order by due_at_est asc limit ?"
    params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "campaign_run_id": r["campaign_run_id"],
                "lead_id": r["lead_id"],
                "type": r["type"],
                "channel": r["channel"],
                "status": r["status"],
                "due_at_est": r["due_at_est"],
                "window_start_est": r["window_start_est"],
                "window_end_est": r["window_end_est"],
                "instructions": r["instructions"],
                "template_id": r["template_id"],
                "tactic_id": r["tactic_id"],
                "experiment_id": r["experiment_id"],
                "completion": json.loads(r["completion_json"] or "{}"),
            }
        )
    return {"tasks": out}


def _log_outreach_attempt(body: OutreachAttemptCreate) -> Dict[str, Any]:
    # basic validation
    allowed_channels = {"email", "phone", "linkedin", "other"}
    allowed_outcomes = {"sent", "delivered", "replied", "positive", "negative", "meeting_booked", "bounced", "unsubscribed"}
    if body.channel not in allowed_channels:
        raise HTTPException(400, f"Invalid channel: {body.channel}")
    if body.outcome_code not in allowed_outcomes:
        raise HTTPException(400, f"Invalid outcome_code: {body.outcome_code}")

    # ensure campaign + lead exist
    cr = get_campaign_run(body.campaign_run_id)
    conn = db()
    lead_row = conn.execute("select id from leads where id=? and campaign_run_id=?", (body.lead_id, body.campaign_run_id)).fetchone()
    if not lead_row:
        conn.close()
        raise HTTPException(404, "Lead not found for campaign run")

    oid = f"oa_{uuid.uuid4().hex}"
    conn.execute(
        "insert into outreach_attempts (id,campaign_run_id,lead_id,channel,template_id,executed_at,outcome_code,outcome_notes,tactic_id,experiment_id,created_at) values (?,?,?,?,?,?,?,?,?,?,?)",
        (
            oid,
            body.campaign_run_id,
            body.lead_id,
            body.channel,
            body.template_id,
            body.executed_at,
            body.outcome_code,
            body.outcome_notes,
            body.tactic_id,
            body.experiment_id,
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()

    # Update campaign progress counters (if present)
    prog = cr.get("progress") or {}
    # ensure keys exist for consistent UI (older runs may not have these counters yet)
    prog["attempts_total"] = int(prog.get("attempts_total") or 0) + 1
    prog["replies"] = int(prog.get("replies") or 0)
    prog["meetings"] = int(prog.get("meetings") or 0)
    if body.outcome_code in {"replied", "positive", "negative", "meeting_booked"}:
        prog["replies"] += 1
    if body.outcome_code == "meeting_booked":
        prog["meetings"] += 1
    update_campaign_run(body.campaign_run_id, progress=prog)

    emit_event(
        body.campaign_run_id,
        "attempt.logged",
        "Outreach attempt logged",
        {
            "outreach_attempt_id": oid,
            "lead_id": body.lead_id,
            "channel": body.channel,
            "template_id": body.template_id,
            "executed_at": body.executed_at,
            "outcome_code": body.outcome_code,
            "tactic_id": body.tactic_id,
            "experiment_id": body.experiment_id,
        },
        lead_id=body.lead_id,
    )

    return {
        "id": oid,
        "campaign_run_id": body.campaign_run_id,
        "lead_id": body.lead_id,
        "channel": body.channel,
        "template_id": body.template_id,
        "executed_at": body.executed_at,
        "outcome_code": body.outcome_code,
        "outcome_notes": body.outcome_notes,
        "tactic_id": body.tactic_id,
        "experiment_id": body.experiment_id,
        "created_at": now_iso(),
    }


@app.post("/v1/outreach-attempts", dependencies=[Depends(auth)])
def create_outreach_attempt(body: OutreachAttemptCreate):
    # Back-compat endpoint (200)
    return _log_outreach_attempt(body)


@app.post("/v1/attempts", status_code=201, dependencies=[Depends(auth)])
def create_attempt(body: OutreachAttemptCreate):
    # Primary endpoint requested for execution logging (201)
    return _log_outreach_attempt(body)


@app.get("/v1/outreach-attempts", dependencies=[Depends(auth)])
def list_outreach_attempts(campaign_run_id: Optional[str] = None, lead_id: Optional[str] = None, limit: int = 200):
    conn = db()
    sql = "select * from outreach_attempts where 1=1"
    params: List[Any] = []
    if campaign_run_id:
        sql += " and campaign_run_id=?"
        params.append(campaign_run_id)
    if lead_id:
        sql += " and lead_id=?"
        params.append(lead_id)
    sql += " order by executed_at desc, created_at desc limit ?"
    params.append(limit)

    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "campaign_run_id": r["campaign_run_id"],
                "lead_id": r["lead_id"],
                "channel": r["channel"],
                "template_id": r["template_id"],
                "executed_at": r["executed_at"],
                "outcome_code": r["outcome_code"],
                "outcome_notes": r["outcome_notes"],
                "created_at": r["created_at"],
            }
        )

    return {"outreach_attempts": out}


@app.patch("/v1/tasks/{task_id}", dependencies=[Depends(auth)])
def patch_task(task_id: str, body: Dict[str, Any]):
    conn = db()
    row = conn.execute("select * from tasks where id=?", (task_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Task not found")

    status = body.get("status", row["status"])
    completion = body.get("completion")
    if completion is None:
        completion = json.loads(row["completion_json"] or "{}")

    conn.execute(
        "update tasks set status=?, completion_json=?, updated_at=? where id=?",
        (status, json.dumps(completion), now_iso(), task_id),
    )
    conn.commit()

    # update campaign progress tasks_completed when a task is done
    if status == "done":
        crid = row["campaign_run_id"]
        cr = get_campaign_run(crid)
        prog = cr["progress"]
        prog["tasks_completed"] += 1
        update_campaign_run(crid, progress=prog)
        emit_event(crid, "task.completed", "Task completed", {"task_id": task_id, "status": status})

    row2 = conn.execute("select * from tasks where id=?", (task_id,)).fetchone()
    conn.close()

    return {
        "id": row2["id"],
        "campaign_run_id": row2["campaign_run_id"],
        "lead_id": row2["lead_id"],
        "type": row2["type"],
        "channel": row2["channel"],
        "status": row2["status"],
        "due_at_est": row2["due_at_est"],
        "window_start_est": row2["window_start_est"],
        "window_end_est": row2["window_end_est"],
        "instructions": row2["instructions"],
        "template_id": row2["template_id"],
        "completion": json.loads(row2["completion_json"] or "{}"),
    }


@app.get("/v1/metrics/overview", dependencies=[Depends(auth)])
def metrics_overview(campaign_run_id: Optional[str] = None):
    """Overview metrics for dashboards.

    Keeps existing fields stable and adds execution/outcome metrics derived from OutreachAttempts.
    """

    conn = db()
    params: List[Any] = []
    where = ""
    if campaign_run_id:
        where = " where campaign_run_id=?"
        params.append(campaign_run_id)

    # ---- Task metrics ----
    total_tasks = conn.execute(f"select count(*) as c from tasks{where}", tuple(params)).fetchone()["c"]
    done_tasks = (
        conn.execute(f"select count(*) as c from tasks{where} and status='done'", tuple(params)).fetchone()["c"]
        if where
        else conn.execute("select count(*) as c from tasks where status='done'").fetchone()["c"]
    )

    # ---- Lead metrics ----
    total_leads = conn.execute(f"select count(*) as c from leads{where}", tuple(params)).fetchone()["c"]
    tier_counts: Dict[str, int] = {}
    for t in ["A", "B", "C", "D"]:
        if where:
            tier_counts[t] = conn.execute(
                "select count(*) as c from leads where campaign_run_id=? and tier=?",
                (campaign_run_id, t),
            ).fetchone()["c"]
        else:
            tier_counts[t] = conn.execute("select count(*) as c from leads where tier=?", (t,)).fetchone()["c"]

    # ---- OutreachAttempt metrics ----
    attempts_total = conn.execute(f"select count(*) as c from outreach_attempts{where}", tuple(params)).fetchone()["c"]

    # attempts by channel
    attempts_by_channel: Dict[str, int] = {"email": 0, "phone": 0, "linkedin": 0, "other": 0}
    ch_rows = conn.execute(
        f"select channel, count(*) as c from outreach_attempts{where} group by channel",
        tuple(params),
    ).fetchall()
    for r in ch_rows:
        attempts_by_channel[r["channel"]] = r["c"]

    # attempts by tactic (requires attribution on OutreachAttempts)
    by_tactic: List[Dict[str, Any]] = []
    if campaign_run_id:
        tr = conn.execute(
            "select tactic_id, count(*) as attempts from outreach_attempts where campaign_run_id=? and tactic_id is not null group by tactic_id order by attempts desc",
            (campaign_run_id,),
        ).fetchall()
        for row in tr:
            tactic_id = row["tactic_id"]
            replies = conn.execute(
                "select count(*) as c from outreach_attempts where campaign_run_id=? and tactic_id=? and outcome_code in (?,?,?,?)",
                (campaign_run_id, tactic_id, "replied", "positive", "negative", "meeting_booked"),
            ).fetchone()["c"]
            meetings = conn.execute(
                "select count(*) as c from outreach_attempts where campaign_run_id=? and tactic_id=? and outcome_code='meeting_booked'",
                (campaign_run_id, tactic_id),
            ).fetchone()["c"]
            by_tactic.append({"tactic_id": tactic_id, "attempts": row["attempts"], "replies": replies, "meetings": meetings})

    reply_codes = ("replied", "positive", "negative", "meeting_booked")
    replies_total = conn.execute(
        f"select count(*) as c from outreach_attempts{where} and outcome_code in (?,?,?,?)" if where else "select count(*) as c from outreach_attempts where outcome_code in (?,?,?,?)",
        tuple(params + list(reply_codes)) if where else reply_codes,
    ).fetchone()["c"]

    positive_codes = ("positive", "meeting_booked")
    positive_replies_total = conn.execute(
        f"select count(*) as c from outreach_attempts{where} and outcome_code in (?,?)" if where else "select count(*) as c from outreach_attempts where outcome_code in (?,?)",
        tuple(params + list(positive_codes)) if where else positive_codes,
    ).fetchone()["c"]

    meetings_total = conn.execute(
        f"select count(*) as c from outreach_attempts{where} and outcome_code='meeting_booked'" if where else "select count(*) as c from outreach_attempts where outcome_code='meeting_booked'",
        tuple(params) if where else (),
    ).fetchone()["c"]

    bounces_total = conn.execute(
        f"select count(*) as c from outreach_attempts{where} and outcome_code='bounced'" if where else "select count(*) as c from outreach_attempts where outcome_code='bounced'",
        tuple(params) if where else (),
    ).fetchone()["c"]

    unsubscribes_total = conn.execute(
        f"select count(*) as c from outreach_attempts{where} and outcome_code='unsubscribed'" if where else "select count(*) as c from outreach_attempts where outcome_code='unsubscribed'",
        tuple(params) if where else (),
    ).fetchone()["c"]

    bounce_rate = (bounces_total / attempts_total) if attempts_total else 0.0
    reply_rate = (replies_total / attempts_total) if attempts_total else 0.0
    meeting_rate = (meetings_total / attempts_total) if attempts_total else 0.0

    # last 7d metrics (based on executed_at)
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat().replace("+00:00", "Z")

    # attempts_last_7d
    attempts_last_7d = conn.execute(
        (f"select count(*) as c from outreach_attempts{where} and executed_at >= ?" if where else "select count(*) as c from outreach_attempts where executed_at >= ?"),
        tuple(params + [cutoff_iso]) if where else (cutoff_iso,),
    ).fetchone()["c"]

    replies_last_7d = conn.execute(
        (f"select count(*) as c from outreach_attempts{where} and executed_at >= ? and outcome_code in (?,?,?,?)" if where else "select count(*) as c from outreach_attempts where executed_at >= ? and outcome_code in (?,?,?,?)"),
        tuple(params + [cutoff_iso] + list(reply_codes)) if where else tuple([cutoff_iso] + list(reply_codes)),
    ).fetchone()["c"]

    meetings_last_7d = conn.execute(
        (f"select count(*) as c from outreach_attempts{where} and executed_at >= ? and outcome_code='meeting_booked'" if where else "select count(*) as c from outreach_attempts where executed_at >= ? and outcome_code='meeting_booked'"),
        tuple(params + [cutoff_iso]) if where else (cutoff_iso,),
    ).fetchone()["c"]

    reply_rate_last_7d = (replies_last_7d / attempts_last_7d) if attempts_last_7d else 0.0
    meeting_rate_last_7d = (meetings_last_7d / attempts_last_7d) if attempts_last_7d else 0.0

    conn.close()

    return {
        # existing fields
        "leads_total": total_leads,
        "tiers": tier_counts,
        "tasks_total": total_tasks,
        "tasks_done": done_tasks,
        "task_completion_rate": (done_tasks / total_tasks) if total_tasks else 0.0,
        # execution/outcome fields
        "attempts_total": attempts_total,
        "attempts_by_channel": attempts_by_channel,
        "attempts_by_tactic": by_tactic,
        "replies_total": replies_total,
        "positive_replies_total": positive_replies_total,
        "meetings_total": meetings_total,
        "bounces_total": bounces_total,
        "unsubscribes_total": unsubscribes_total,
        "bounce_rate": bounce_rate,
        "reply_rate": reply_rate,
        "meeting_rate": meeting_rate,
        # last 7d
        "attempts_last_7d": attempts_last_7d,
        "replies_last_7d": replies_last_7d,
        "meetings_last_7d": meetings_last_7d,
        "reply_rate_last_7d": reply_rate_last_7d,
        "meeting_rate_last_7d": meeting_rate_last_7d,
        "last_7d_cutoff_utc": cutoff_iso,
    }
