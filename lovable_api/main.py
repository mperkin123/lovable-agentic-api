from __future__ import annotations

import csv
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
        """
    )
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


def _llm_business_profile(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Call OpenAI if configured; otherwise return defaults."""
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
        r.raise_for_status()
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

    total = min(100, loc_points + size_points + industry_points)
    tier = "A" if total >= 80 else "B" if total >= 65 else "C" if total >= 50 else "D"
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
    return total, tier, reasons, components


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

            # Score
            total, tier, reasons, components = _score_stub(lead, criteria)
            lead["score"] = {"total": total, "tier": tier, "components": components, "reasoning_bullets": reasons}
            prog["leads_scored"] = min(prog["leads_scored"] + 1, prog.get("seed_rows_total") or prog["leads_scored"] + 1)
            emit_event(campaign_run_id, "lead.scored", "Lead scored", {"total": total, "tier": tier}, lead_id=lead_id)

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

        # Tasks for A/B
        if tier in ("A", "B"):
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
            lead_id = r["id"]
            lead = json.loads(r["lead_json"])

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
    return json.loads(row["lead_json"])


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
        "insert into outreach_attempts (id,campaign_run_id,lead_id,channel,template_id,executed_at,outcome_code,outcome_notes,created_at) values (?,?,?,?,?,?,?,?,?)",
        (
            oid,
            body.campaign_run_id,
            body.lead_id,
            body.channel,
            body.template_id,
            body.executed_at,
            body.outcome_code,
            body.outcome_notes,
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
