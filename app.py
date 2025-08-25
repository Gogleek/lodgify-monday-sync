import os, json, logging, re, requests, time, uuid
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)
level = os.getenv("LOG_LEVEL", "INFO").upper()
handler = RotatingFileHandler("app.log", maxBytes=1_000_000, backupCount=3)
logging.basicConfig(level=level, handlers=[handler, logging.StreamHandler()])
log = logging.getLogger("lodgify-monday")

def _rid(): return uuid.uuid4().hex[:12]

# trace external HTTP a little
_original_request = requests.sessions.Session.request
def _request(self, method, url, **kw):
    rid = _rid()
    if "lodgify.com" in url: log.info("[Lodgify %s] %s %s params=%s", rid, method, url, kw.get("params"))
    if "monday.com"  in url: log.info("[Monday  %s] %s %s", rid, method, url)
    return _original_request(self, method, url, **kw)
requests.sessions.Session.request = _request

E164_RE = re.compile(r"^\+?[1-9]\d{6,14}$")
def normalize_phone(raw: str) -> str:
    if not raw: return ""
    s = str(raw).replace("(0)", "")
    s = re.sub(r"[\s\-().]", "", s)
    if s.startswith("00"): s = "+" + s[2:]
    if E164_RE.match(s): return s
    d = re.sub(r"\D", "", s)
    return d[-12:] if d else ""

def iso_date(v) -> Optional[str]:
    if not v: return None
    if isinstance(v, dict):
        v = v.get("time") or v.get("date")
        if not v: return None
    s = str(v)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        try: return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
        except Exception: return None

def safe_float(x, default=0.0):
    try: return float(x)
    except Exception: return float(default)

def days_between(a: Optional[str], b: Optional[str]) -> Optional[int]:
    if not a or not b: return None
    try:
        da = datetime.strptime(a, "%Y-%m-%d").date()
        db = datetime.strptime(b, "%Y-%m-%d").date()
        return (db - da).days
    except Exception: return None

def today_iso(): return datetime.now(timezone.utc).date().isoformat()

def parse_unit_from_source_text(source_text: str) -> Optional[str]:
    if not source_text: return None
    m = re.search(r"\(([^)]+)\)\s*$", source_text.strip())
    if m and m.group(1).strip(): return m.group(1).strip()
    m2 = re.search(r":\s*([^:]+)$", source_text.strip())
    if m2:
        cand = m2.group(1).strip()
        if cand and len(cand) <= 64: return cand
    return None

COLUMN_MAP = {
    "reservation_id": "text_mkv47vb1",
    "unit":           "text_mkv49eqm",
    "property_id":    "text_mkv4n35j",
    "guest_name":     "text_mkv46pev",
    "email":          "email_mkv4mbte",
    "phone":          "phone_mkv4yk8k",
    "check_in":       "date_mkv4npgx",
    "check_out":      "date_mkv46w1t",
    "nights":         "numeric_mkv4j5aq",
    "source":         "dropdown_mkv47kzc",
    "status":         "color_mkv4zrs6",
    "last_sync":      "date_mkv44erw",
    "raw_json":       "long_text_mkv4y19w",
    "booking_status": "text_mkv4kjxs",
    "currency":       "text_mkv497t1",
    "total":          "numeric_mkv4n3qy",
    "amount_paid":    "numeric_mkv43src",
    "amount_due":     "numeric_mkv4zk73",
    "source_text":    "long_text_mkv435cw",
    "language":       "text_mkv41dhj",
    "adults":         "numeric_mkv4nhza",
    "children":       "numeric_mkv4dq38",
    "infants":        "numeric_mkv4ez6r",
    "pets":           "numeric_mkv49d8e",
    "people":         "numeric_mkv4z385",
    "key_code":       "text_mkv4ae9w",
    "thread_uid":     "text_mkv49b55",
    "created_at":     "date_mkv4bkr9",
    "updated_at":     "date_mkv4n357",
    "canceled_at":    "date_mkv4hw1d",
    # NEW lifecycle
    "stay_lifecycle": "color_mkv4v5f0",
}

STATUS_LABELS = {
    "confirmed": "Confirmed",
    "booked":    "Confirmed",
    "paid":      "Paid",
    "pending":   "Pending",
    "cancelled": "Cancelled",
    "canceled":  "Cancelled",
}
STATUS_DEFAULT = "Pending"

SOURCE_LABELS = {
    "booking.com": "Booking.com",
    "airbnb": "Airbnb",
    "expedia": "Expedia",
    "vrbo": "Vrbo",
}

def put(cv: dict, logical_key: str, value):
    col_id = COLUMN_MAP.get(logical_key)
    if col_id is not None and value is not None:
        cv[col_id] = value

# ---------------- Lodgify with property/roomtype cache ----------------
class LodgifyClient:
    def __init__(self, api_base: str, api_key: str):
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-ApiKey": self.api_key,
        })
        self.property_cache: Dict[str, Optional[str]] = {}
        self.roomtype_cache: Dict[str, Optional[str]] = {}

    def _get_json_first_ok(self, paths: List[str]) -> Optional[dict]:
        for p in paths:
            try:
                r = self.session.get(f"{self.api_base}{p}", timeout=20)
                if r.ok: return r.json()
            except Exception: pass
        return None

    def _pluck_name(self, obj: dict) -> Optional[str]:
        if isinstance(obj, dict):
            for k in ("name","title","displayName","display_name"):
                v = obj.get(k)
                if isinstance(v,str) and v.strip(): return v.strip()
            d = obj.get("data")
            if isinstance(d, dict):
                for k in ("name","title","displayName","display_name"):
                    v = d.get(k)
                    if isinstance(v,str) and v.strip(): return v.strip()
        return None

    def list_bookings(self, limit: int = 50, skip: int = 0) -> List[dict]:
        url = f"{self.api_base}/v2/reservations/bookings"
        params = {"take": max(1,int(limit)), "skip": max(0,int(skip))}
        resp = self.session.get(url, params=params, timeout=45)
        if resp.status_code in (400,404):
            page_size = max(1,int(limit))
            page_number = max(1,(int(skip)//page_size)+1)
            params = {"pageSize": page_size, "pageNumber": page_number}
            resp = self.session.get(url, params=params, timeout=45)
        if not resp.ok:
            raise RuntimeError(f"Lodgify error {resp.status_code}: {resp.text[:500]}")
        data = resp.json() or {}
        items = data.get("results") or data.get("items") or data.get("data") or data
        if isinstance(items, dict): items = list(items.values())
        if not isinstance(items, list): items = []
        log.info("Lodgify v2 fetched %d item(s) (limit=%s, skip=%s)", len(items), limit, skip)
        return items

    def get_property_name(self, property_id: Optional[int]) -> Optional[str]:
        if not property_id: return None
        k = str(property_id)
        if k in self.property_cache: return self.property_cache[k]
        data = self._get_json_first_ok([
            f"/v2/rentals/{property_id}",
            f"/v2/properties/{property_id}",
            f"/v1/properties/{property_id}",
            f"/v1/rentals/{property_id}",
        ])
        name = self._pluck_name(data) if data else None
        self.property_cache[k] = name
        if name: log.info("Resolved property %s → %s", property_id, name)
        return name

    def get_roomtype_name(self, room_type_id: Optional[int], property_id: Optional[int] = None) -> Optional[str]:
        if not room_type_id: return None
        k = str(room_type_id)
        if k in self.roomtype_cache: return self.roomtype_cache[k]
        paths = [f"/v2/roomtypes/{room_type_id}", f"/v1/roomtypes/{room_type_id}"]
        if property_id:
            paths = [f"/v2/rentals/{property_id}/roomtypes/{room_type_id}",
                     f"/v1/rentals/{property_id}/roomtypes/{room_type_id}"] + paths
        data = self._get_json_first_ok(paths)
        name = self._pluck_name(data) if data else None
        self.roomtype_cache[k] = name
        if name: log.info("Resolved roomtype %s → %s", room_type_id, name)
        return name

    def resolve_unit_name(self, property_id: Optional[int], room_type_id: Optional[int],
                          source_text: Optional[str], fallback: Optional[str]) -> Optional[str]:
        n1 = parse_unit_from_source_text(source_text or "")
        if n1: return n1
        n2 = self.get_roomtype_name(room_type_id, property_id)
        if n2: return n2
        n3 = self.get_property_name(property_id)
        if n3: return n3
        return (fallback or "").strip() or None

lodgify: "LodgifyClient"

# ---------------- Monday ----------------
@dataclass
class UpsertResult:
    ok: bool
    item_id: Optional[int] = None
    created: bool = False
    updated: bool = False
    error: Optional[str] = None
    def to_dict(self): return {"ok": self.ok, "item_id": self.item_id, "created": self.created, "updated": self.updated, "error": self.error}

class MondayClient:
    def __init__(self, api_base: str, api_key: str, board_id: int):
        self.api_base = api_base; self.api_key = api_key; self.board_id = board_id
        self.session = requests.Session()
        self.session.headers.update({"Authorization": self.api_key, "Content-Type": "application/json"})

    def _gql(self, query: str, variables: dict = None) -> dict:
        payload = {"query": query}; 
        if variables: payload["variables"] = variables
        r = self.session.post(self.api_base, data=json.dumps(payload), timeout=45)
        if r.status_code != 200: raise RuntimeError(f"Monday HTTP {r.status_code}: {r.text[:500]}")
        out = r.json()
        if "errors" in out: raise RuntimeError(f"Monday GQL error: {out['errors']}")
        return out.get("data", {})

    def find_item_by_external_id(self, column_id: str, external_id: str) -> Optional[int]:
        q = """
        query($board_id: ID!, $column_id: String!, $value: String!) {
          items_page_by_column_values(
            board_id: $board_id,
            columns: [{column_id: $column_id, column_values: [$value]}],
            limit: 1
          ) { items { id } }
        }"""
        data = self._gql(q, {"board_id": str(self.board_id), "column_id": column_id, "value": external_id})
        items = (((data or {}).get("items_page_by_column_values") or {}).get("items")) or []
        return int(items[0]["id"]) if items else None

    def create_item(self, item_name: str, column_values: Dict[str, object]) -> int:
        q = """mutation($board_id: ID!, $name: String!, $cols: JSON!) {
          create_item(board_id: $board_id, item_name: $name, column_values: $cols) { id }
        }"""
        data = self._gql(q, {"board_id": str(self.board_id), "name": item_name, "cols": json.dumps(column_values)})
        return int(data["create_item"]["id"])

    def update_item(self, item_id: int, column_values: Dict[str, object]) -> int:
        q = """mutation($item_id: ID!, $board_id: ID!, $cols: JSON!) {
          change_multiple_column_values(item_id: $item_id, board_id: $board_id, column_values: $cols) { id }
        }"""
        data = self._gql(q, {"item_id": str(item_id), "board_id": str(self.board_id), "cols": json.dumps(column_values)})
        return int(data["change_multiple_column_values"]["id"])

    def upsert_item(self, mapped: dict) -> UpsertResult:
        item_name = mapped["item_name"]; external_id = mapped["external_id"]; column_values = mapped["column_values"]
        lookup_col = COLUMN_MAP["reservation_id"]
        try:
            try:
                existing_id = self.find_item_by_external_id(lookup_col, external_id)
            except Exception as e:
                if "Column not found" in str(e) or "missing_column" in str(e):
                    log.warning("Lookup column '%s' not found on board %s. Creating without lookup.", lookup_col, self.board_id)
                    existing_id = None
                else:
                    raise
            if existing_id:
                self.update_item(existing_id, column_values)
                log.info("Updated Monday item id=%s (ext=%s)", existing_id, external_id)
                return UpsertResult(ok=True, item_id=existing_id, updated=True)
            else:
                safe_name = f"{item_name} • #{external_id}"
                new_id = self.create_item(safe_name, column_values)
                log.info("Created Monday item id=%s (ext=%s)", new_id, external_id)
                return UpsertResult(ok=True, item_id=new_id, created=True)
        except Exception as e:
            log.exception("Upsert failed for external_id=%s", external_id)
            return UpsertResult(ok=False, error=str(e))

# --------- mapping helpers ----------
def label_for_status(raw: str) -> str:
    key = (raw or "").lower().strip()
    return STATUS_LABELS.get(key, STATUS_DEFAULT)

def deduce_source(bk: dict) -> (Optional[str], Optional[str]):
    """
    Try hard to figure out OTA. Returns (dropdown_label or None, raw_text or None)
    """
    cand = []
    # direct fields
    for k in ("source", "source_text", "ota", "origin", "channel"):
        v = bk.get(k)
        if isinstance(v, str) and v.strip(): cand.append(v.strip())
    # nested
    ext = bk.get("external_booking") or {}
    if isinstance(ext, dict):
        for k in ("source", "source_text", "channel", "origin", "ota", "provider"):
            v = ext.get(k)
            if isinstance(v, str) and v.strip(): cand.append(v.strip())
    # guest email heuristic
    gemail = ((bk.get("guest") or {}).get("email") or "").lower()
    if "guest.booking.com" in gemail: cand.append("booking.com")
    raw = " ".join(cand).strip()
    raw_l = raw.lower()
    for key, lbl in SOURCE_LABELS.items():
        if key in raw_l:
            return lbl, raw or None
    # manual/direct stays go to raw text (dropdown არ ვაშლევინებთ)
    if "manual" in raw_l:  return None, raw or "Manual"
    if "direct" in raw_l:  return None, raw or "Direct"
    return None, raw or None

def lifecycle_label(check_in: Optional[str], check_out: Optional[str], status_raw: str) -> str:
    today = today_iso()
    cancelled = (status_raw or "").lower().strip() in {"cancelled", "canceled"}
    if check_in and check_out:
        if today < check_in: return "Upcoming"
        if check_in <= today < check_out and not cancelled: return "In house"
        return "Completed"
    return "Completed" if cancelled else "Upcoming"

def map_booking_to_monday(bk: dict) -> dict:
    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "")
    property_id = bk.get("property_id") or (bk.get("rental") or {}).get("id")
    source_text = (bk.get("source_text") or "").strip()

    # unit
    unit_name = parse_unit_from_source_text(source_text) \
                or (bk.get("rental") or {}).get("name") \
                or bk.get("unit_name") \
                or None

    guest = bk.get("guest") or {}
    full_name = (guest.get("name") or "").strip()
    first_name = (guest.get("first_name") or "").strip()
    last_name = (guest.get("last_name") or "").strip()
    if not (first_name or last_name) and full_name:
        parts = full_name.split()
        first_name = parts[0] if len(parts)==1 else " ".join(parts[:-1]); last_name = "" if len(parts)==1 else parts[-1]
    display_name = (f"{first_name} {last_name}".strip() or full_name) or f"Booking {res_id}"
    email = (guest.get("email") or "").strip()
    phone = normalize_phone(guest.get("phone") or guest.get("mobile") or "")

    check_in  = iso_date(bk.get("arrival") or bk.get("check_in"))
    check_out = iso_date(bk.get("departure") or bk.get("check_out"))
    nights = days_between(check_in, check_out)

    total_amount = safe_float(bk.get("total_amount") or bk.get("total") or bk.get("price_total"))
    amount_paid = safe_float(bk.get("amount_paid"))
    amount_due  = safe_float(bk.get("amount_due"))
    currency = bk.get("currency_code") or bk.get("currency") or "GBP"

    status_raw = bk.get("status") or ""
    status_label = label_for_status(status_raw)

    rooms = bk.get("rooms") or []
    room_type_id = None
    people = None; adults = children = infants = pets = None; key_code = None
    if rooms:
        r0 = rooms[0]
        room_type_id = r0.get("room_type_id")
        gb = r0.get("guest_breakdown") or {}
        adults  = gb.get("adults"); children = gb.get("children"); infants = gb.get("infants"); pets = gb.get("pets")
        people  = r0.get("people") or (adults or 0)+(children or 0)+(infants or 0)+(pets or 0)
        key_code = r0.get("key_code") or ""

    if (not unit_name) or unit_name.lower().startswith("unknown"):
        unit_name = lodgify.resolve_unit_name(property_id, room_type_id, source_text, unit_name) \
                    or (str(property_id) if property_id else "Unknown unit")

    src_label, src_raw_text = deduce_source(bk)

    cv = {}
    put(cv, "reservation_id", res_id)
    put(cv, "unit", unit_name)
    put(cv, "property_id", str(property_id) if property_id else None)
    put(cv, "guest_name", display_name)
    if email: put(cv, "email", {"email": email, "text": email})
    put(cv, "phone", phone if phone else None)
    put(cv, "check_in", {"date": check_in} if check_in else None)
    put(cv, "check_out", {"date": check_out} if check_out else None)
    put(cv, "nights", nights)
    if src_label: put(cv, "source", {"labels": [src_label]})
    if src_raw_text: put(cv, "source_text", src_raw_text)
    put(cv, "status", {"label": status_label})
    put(cv, "stay_lifecycle", {"label": lifecycle_label(check_in, check_out, status_raw)})
    put(cv, "last_sync", {"date": today_iso()})
    put(cv, "currency", currency)
    put(cv, "total", total_amount)
    put(cv, "amount_paid", amount_paid)
    put(cv, "amount_due", amount_due)
    put(cv, "language", bk.get("language"))
    put(cv, "adults", adults); put(cv, "children", children); put(cv, "infants", infants); put(cv, "pets", pets); put(cv, "people", people)
    put(cv, "key_code", key_code); put(cv, "thread_uid", bk.get("thread_uid"))
    put(cv, "created_at", {"date": iso_date(bk.get("created_at"))} if bk.get("created_at") else None)
    put(cv, "updated_at", {"date": iso_date(bk.get("updated_at"))} if bk.get("updated_at") else None)
    put(cv, "canceled_at", {"date": iso_date(bk.get("canceled_at"))} if bk.get("canceled_at") else None)
    put(cv, "booking_status", (status_raw or "").strip())

    try:
        put(cv, "raw_json", json.dumps(bk, separators=(",", ":"), ensure_ascii=False)[:50000])
    except Exception: pass

    return {"item_name": display_name, "external_id": res_id, "column_values": cv}

# ---------------- Flask ----------------
LODGY_API_BASE = os.getenv("LODGY_API_BASE", "https://api.lodgify.com")
LODGY_API_KEY  = os.getenv("LODGY_API_KEY", "")
MONDAY_API_BASE = os.getenv("MONDAY_API_BASE", "https://api.monday.com/v2")
MONDAY_API_KEY  = os.getenv("MONDAY_API_KEY", "")
try: MONDAY_BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "0"))
except Exception: MONDAY_BOARD_ID = 0

lodgify = LodgifyClient(api_base=LODGY_API_BASE, api_key=LODGY_API_KEY)
monday  = MondayClient(api_base=MONDAY_API_BASE, api_key=MONDAY_API_KEY, board_id=MONDAY_BOARD_ID)

@app.errorhandler(Exception)
def _unhandled(e):
    log.exception("Unhandled error")
    return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/health")
def health():
    env = {
        "LODGY_API_BASE": LODGY_API_BASE,
        "LODGY_API_KEY_set": bool(LODGY_API_KEY),
        "MONDAY_API_BASE": MONDAY_API_BASE,
        "MONDAY_API_KEY_set": bool(MONDAY_API_KEY),
    }
    return jsonify({"ok": True, "service": "lodgify-monday", "board_id": MONDAY_BOARD_ID, "ready": True, "env": env}), 200

@app.get("/")
def root():
    return jsonify({"ok": True, "endpoints": ["/health", "/lodgify-sync-all", "/webhook/lodgify", "/diag/monday-columns"]}), 200

@app.get("/diag/monday-columns")
def diag_monday_columns():
    try:
        q = """query($board_id: [ID!]) { boards(ids: $board_id) { id name columns { id title type } } }"""
        data = monday._gql(q, {"board_id": [str(MONDAY_BOARD_ID)]})
        boards = (data or {}).get("boards") or []
        cols = boards[0]["columns"] if boards else []
        slim = [{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols]
        return jsonify({"ok": True, "board_id": MONDAY_BOARD_ID, "columns": slim}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/webhook/lodgify")
def webhook_lodgify():
    payload = request.get_json(silent=True) or {}
    log.info("Webhook/Lodgify: %s", json.dumps(payload)[:2000])
    booking = payload.get("booking") or payload
    if not booking:
        return jsonify({"ok": False, "error": "No booking payload"}), 400
    mapped = map_booking_to_monday(booking)
    res: UpsertResult = monday.upsert_item(mapped)
    return jsonify({"ok": True, "result": res.to_dict(), "source": "webhook"}), 200

@app.get("/lodgify-sync-all")
def lodgify_sync_all():
    limit = int(request.args.get("limit", 50))
    skip = int(request.args.get("skip", 0))
    debug = request.args.get("debug", "0") == "1"
    max_sec = int(request.args.get("max_sec", 25))
    started = time.time()

    bookings = lodgify.list_bookings(limit=limit, skip=skip)[:limit]
    results = []; processed = 0
    for bk in bookings:
        if (time.time() - started) > max_sec: break
        try:
            mapped = map_booking_to_monday(bk)
            res: UpsertResult = monday.upsert_item(mapped)
            results.append(res.to_dict()); processed += 1
        except Exception as e:
            log.exception("Upsert failed for booking id=%s", bk.get("id"))
            results.append({"ok": False, "error": str(e), "source_id": bk.get("id")})

    resp = {"ok": True, "count": len(results), "processed": processed, "next_skip": skip + processed, "results": results}
    if debug and bookings:
        resp["sample_input"] = bookings[:1]
        resp["sample_mapped"] = map_booking_to_monday(bookings[0])
    return jsonify(resp), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
