import os, json, logging, re, requests, time
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime, timezone, date, timedelta
from flask import Flask, request, jsonify

# -----------------------
# App / Logging
# -----------------------
app = Flask(__name__)
level = os.getenv("LOG_LEVEL", "INFO").upper()
handler = RotatingFileHandler("app.log", maxBytes=1_000_000, backupCount=3)
logging.basicConfig(level=level, handlers=[handler, logging.StreamHandler()])
log = logging.getLogger("lodgify-monday")

# default timeout wrapper (ერთ ადგილზე)
_original_request = requests.Session.request
def _request(self, method, url, **kwargs):
    kwargs.setdefault("timeout", 45)
    return _original_request(self, method, url, **kwargs)
requests.Session.request = _request

# -----------------------
# Helpers
# -----------------------
E164_RE = re.compile(r"^\+?[1-9]\d{6,14}$")

def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw)
    s = s.replace("(0)", "")
    s = re.sub(r"[\s\-().]", "", s)
    if s.startswith("00"):
        s = "+" + s[2:]
    if E164_RE.match(s):
        return s
    digits = re.sub(r"\D", "", s)
    return digits[-12:] if digits else ""

def iso_date(v) -> Optional[str]:
    if not v:
        return None
    if isinstance(v, dict):
        v = v.get("time") or v.get("date") or None
        if not v:
            return None
    s = str(v)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
        except Exception:
            return None

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)

def days_between(a: Optional[str], b: Optional[str]) -> Optional[int]:
    if not a or not b:
        return None
    try:
        da = datetime.strptime(a, "%Y-%m-%d").date()
        db = datetime.strptime(b, "%Y-%m-%d").date()
        return (db - da).days
    except Exception:
        return None

def today_iso():
    return datetime.now(timezone.utc).date().isoformat()

def calc_stay_phase(check_in: Optional[str], check_out: Optional[str]) -> Optional[str]:
    if not check_in or not check_out:
        return None
    t = datetime.now(timezone.utc).date()
    ci = datetime.strptime(check_in, "%Y-%m-%d").date()
    co = datetime.strptime(check_out, "%Y-%m-%d").date()
    if t < ci:
        return "Upcoming"
    if ci <= t < co:
        return "In house"
    return "Completed"

# -----------------------
# COLUMN MAP ( შენი IDs )
# -----------------------
COLUMN_MAP = {
    "reservation_id": "text_mkv47vb1",     # Booking ID (Text) — lookup key
    "unit":           "text_mkv49eqm",     # Property / Unit (Text)
    "property_id":    "text_mkv4n35j",     # Property ID (Text)
    "guest_name":     "text_mkv46pev",     # Guest (Text)
    "email":          "email_mkv4mbte",    # Email
    "phone":          "phone_mkv4yk8k",    # Phone
    "check_in":       "date_mkv4npgx",     # Check-in (Date)
    "check_out":      "date_mkv46w1t",     # Check-out (Date)
    "nights":         "numeric_mkv4j5aq",  # Nights (Numbers)
    "source":         "dropdown_mkv47kzc", # Source (Dropdown)
    "status":         "color_mkv4zrs6",    # Status (Status/color)
    "stay_status":    "color_mkv4v5f0",    # NEW: Upcoming / In house / Completed
    "last_sync":      "date_mkv44erw",     # Last Sync (Date)
    "raw_json":       "long_text_mkv4y19w",# Raw JSON (Long text)
    "booking_status": "text_mkv4kjxs",     # Booking Status (Text)
    "currency":       "text_mkv497t1",     # Currency (Text)
    "total":          "numeric_mkv4n3qy",  # Total Amount (Numbers)
    "amount_paid":    "numeric_mkv43src",  # Amount Paid (Numbers)
    "amount_due":     "numeric_mkv4zk73",  # Amount Due (Numbers)
    "source_text":    "long_text_mkv435cw",# Source Text (Long text)
    "language":       "text_mkv41dhj",     # Language (Text)
    "adults":         "numeric_mkv4nhza",  # Adults (Numbers)
    "children":       "numeric_mkv4dq38",  # Children (Numbers)
    "infants":        "numeric_mkv4ez6r",  # Infants (Numbers)
    "pets":           "numeric_mkv49d8e",  # Pets (Numbers)
    "people":         "numeric_mkv4z385",  # People (Numbers)
    "key_code":       "text_mkv4ae9w",     # Key Code (Text)
    "thread_uid":     "text_mkv49b55",     # Thread UID (Text)
    "created_at":     "date_mkv4bkr9",     # Created At (Date)
    "updated_at":     "date_mkv4n357",     # Updated At (Date)
    "canceled_at":    "date_mkv4hw1d",     # Canceled At (Date)
}

# Label maps
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
    # dropdown labels that რეალურად გაქვს ბორდზე
    "booking.com": "Booking.com",
    "airbnb":      "Airbnb",
    "expedia":     "Expedia",
    "vrbo":        "Vrbo",
}
# "manual" და სხვა აუცნობი არდავარდეს dropdown-ში — წავა source_text-ში

def put(cv: dict, logical_key: str, value):
    col_id = COLUMN_MAP.get(logical_key)
    if col_id is not None and value is not None:
        cv[col_id] = value

# -----------------------
# Lodgify Client (v2)
# -----------------------
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

    def list_bookings(self, limit: int = 50, skip: int = 0) -> List[dict]:
        url = f"{self.api_base}/v2/reservations/bookings"
        params = {"take": max(1, int(limit)), "skip": max(0, int(skip))}
        log.info("[Lodgify] GET %s params=%s", url, params)
        resp = self.session.get(url, params=params)

        # fallback: page/pageSize სტილი
        if resp.status_code in (400, 404):
            page_size = max(1, int(limit))
            page_number = max(1, (int(skip) // page_size) + 1)
            params = {"pageSize": page_size, "pageNumber": page_number}
            log.info("[Lodgify] fallback GET %s params=%s", url, params)
            resp = self.session.get(url, params=params)

        if not resp.ok:
            raise RuntimeError(f"Lodgify error {resp.status_code}: {resp.text[:500]}")

        data = resp.json() or {}
        items = data.get("results") or data.get("items") or data.get("data") or data
        if isinstance(items, dict):
            items = list(items.values())
        if not isinstance(items, list):
            items = []
        log.info("[Lodgify] fetched %d item(s) (limit=%s, skip=%s)", len(items), limit, skip)
        return items

# -----------------------
# Monday Client
# -----------------------
@dataclass
class UpsertResult:
    ok: bool
    item_id: Optional[int] = None
    created: bool = False
    updated: bool = False
    error: Optional[str] = None
    def to_dict(self):
        return {"ok": self.ok, "item_id": self.item_id, "created": self.created, "updated": self.updated, "error": self.error}

class MondayClient:
    def __init__(self, api_base: str, api_key: str, board_id: int):
        self.api_base = api_base
        self.api_key = api_key
        self.board_id = board_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": self.api_key,  # raw token
            "Content-Type": "application/json",
        })

    def _gql(self, query: str, variables: dict = None) -> dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        r = self.session.post(self.api_base, data=json.dumps(payload))
        if r.status_code != 200:
            raise RuntimeError(f"Monday HTTP {r.status_code}: {r.text[:500]}")
        out = r.json()
        if "errors" in out:
            raise RuntimeError(f"Monday GQL error: {out['errors']}")
        return out.get("data", {})

    def find_item_by_external_id(self, column_id: str, external_id: str) -> Optional[dict]:
        query = """
        query($board_id: ID!, $column_id: String!, $value: String!) {
          items_page_by_column_values(
            board_id: $board_id,
            columns: [{column_id: $column_id, column_values: [$value]}],
            limit: 1
          ) { items { id state } }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "column_id": column_id, "value": external_id})
        items = (((data or {}).get("items_page_by_column_values") or {}).get("items")) or []
        if not items:
            return None
        it = items[0]
        return {"id": int(it["id"]), "state": it.get("state") or "active"}

    def create_item(self, item_name: str, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $name: String!, $cols: JSON!) {
          create_item(board_id: $board_id, item_name: $name, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "name": item_name, "cols": json.dumps(column_values)})
        return int(data["create_item"]["id"])

    def update_item(self, item_id: int, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $item_id: ID!, $cols: JSON!) {
          change_multiple_column_values(item_id: $item_id, board_id: $board_id, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "item_id": str(item_id), "cols": json.dumps(column_values)})
        return int(data["change_multiple_column_values"]["id"])

    def restore_item(self, item_id: int) -> bool:
        query = """mutation($item_id: ID!) { restore_item(item_id: $item_id) { id } }"""
        try:
            self._gql(query, {"item_id": str(item_id)})
            return True
        except Exception as e:
            log.warning("Restore failed for item %s: %s", item_id, e)
            return False

    def upsert_item(self, mapped: dict) -> UpsertResult:
        item_name = mapped["item_name"]
        external_id = mapped["external_id"]
        column_values = mapped["column_values"]

        lookup_col = COLUMN_MAP["reservation_id"]
        try:
            existing_meta = None
            try:
                existing_meta = self.find_item_by_external_id(lookup_col, external_id)
            except Exception as inner_e:
                if "Column not found" in str(inner_e) or "missing_column" in str(inner_e):
                    log.warning("Lookup column '%s' not found on board %s. Creating without lookup.", lookup_col, self.board_id)
                else:
                    raise

            if existing_meta:
                item_id = existing_meta["id"]
                state = (existing_meta.get("state") or "active").lower()

                if state == "active":
                    self.update_item(item_id, column_values)
                    log.info("Updated Monday item id=%s (ext=%s)", item_id, external_id)
                    return UpsertResult(ok=True, item_id=item_id, created=False, updated=True)

                if state == "archived":
                    if self.restore_item(item_id):
                        self.update_item(item_id, column_values)
                        log.info("Restored & updated item id=%s (ext=%s)", item_id, external_id)
                        return UpsertResult(ok=True, item_id=item_id, created=False, updated=True)
                    else:
                        safe_name = f"{item_name} • #{external_id}"
                        new_id = self.create_item(safe_name, column_values)
                        log.info("Created NEW (restore failed) id=%s (ext=%s)", new_id, external_id)
                        return UpsertResult(ok=True, item_id=new_id, created=True, updated=False)

                if state == "deleted":
                    safe_name = f"{item_name} • #{external_id}"
                    new_id = self.create_item(safe_name, column_values)
                    log.info("Created NEW (was deleted) id=%s (ext=%s)", new_id, external_id)
                    return UpsertResult(ok=True, item_id=new_id, created=True, updated=False)

                safe_name = f"{item_name} • #{external_id}"
                new_id = self.create_item(safe_name, column_values)
                log.info("Created NEW (unknown state=%s) id=%s (ext=%s)", state, new_id, external_id)
                return UpsertResult(ok=True, item_id=new_id, created=True, updated=False)

            # საერთოდ ვერ ვიპოვეთ — შექმენი
            safe_name = f"{item_name} • #{external_id}"
            new_id = self.create_item(safe_name, column_values)
            log.info("Created Monday item id=%s (ext=%s)", new_id, external_id)
            return UpsertResult(ok=True, item_id=new_id, created=True, updated=False)

        except Exception as e:
            log.exception("Upsert failed for external_id=%s", external_id)
            return UpsertResult(ok=False, error=str(e))

# -----------------------
# Mapping Lodgify → Monday
# -----------------------
UNIT_IN_PARENS_RE = re.compile(r"\(([^()]{1,60})\)\s*$")
GENERIC_UNIT_RE = re.compile(r"\b(Queen'?s\s+\d+|Samui\s+\d+\/\d+|B\d+|Cornwall|Rutland)\b", re.IGNORECASE)

def guess_unit_name(bk: dict) -> Optional[str]:
    # 1) explicit fields
    unit_name = (bk.get("rental") or {}).get("name") or bk.get("unit_name")
    if unit_name and isinstance(unit_name, str) and unit_name.strip().lower() not in {"null", "none", "false"}:
        return unit_name.strip()

    # 2) source_text ბოლოში (Queens 8)
    st = (bk.get("source_text") or "").strip()
    if st:
        m = UNIT_IN_PARENS_RE.search(st)
        if m:
            return m.group(1).strip()
        m2 = GENERIC_UNIT_RE.search(st)
        if m2:
            return m2.group(0).strip()

    # 3) არაფერია — Unknown unit
    return "Unknown unit"

def label_for_status(raw: str) -> str:
    key = (raw or "").lower().strip()
    return STATUS_LABELS.get(key, STATUS_DEFAULT)

def label_for_source(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    r = raw.lower()
    for k, v in SOURCE_LABELS.items():
        if k in r:
            return v
    return None  # აუცნობი — dropdown არ შევავსოთ

def map_booking_to_monday(bk: dict) -> dict:
    # identifiers & meta
    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "")
    property_id = bk.get("property_id") or (bk.get("rental") or {}).get("id")
    unit_name = guess_unit_name(bk)

    # guest
    guest = bk.get("guest") or {}
    full_name = (guest.get("name") or "").strip()
    first_name = (guest.get("first_name") or "").strip()
    last_name = (guest.get("last_name") or "").strip()
    if not (first_name or last_name) and full_name:
        parts = full_name.split()
        if len(parts) == 1:
            first_name = parts[0]
        else:
            first_name = " ".join(parts[:-1]); last_name = parts[-1]
    display_name = (f"{first_name} {last_name}".strip() or full_name) or f"Booking {res_id}"
    email = (guest.get("email") or "") or None
    phone = normalize_phone(guest.get("phone") or guest.get("mobile") or "")

    # dates
    check_in = iso_date(bk.get("arrival") or bk.get("check_in"))
    check_out = iso_date(bk.get("departure") or bk.get("check_out"))
    nights = days_between(check_in, check_out)
    stay_phase = calc_stay_phase(check_in, check_out)

    # money
    total_amount = safe_float(bk.get("total_amount") or bk.get("total") or bk.get("price_total"))
    amount_paid = safe_float(bk.get("amount_paid"))
    amount_due  = safe_float(bk.get("amount_due"))
    currency = bk.get("currency_code") or bk.get("currency") or "GBP"

    # status / source
    status_raw = bk.get("status") or ""
    status_label = label_for_status(status_raw)
    source_text = (bk.get("source_text") or "").strip()
    source_raw  = (bk.get("source") or "") + (" " + source_text if source_text else "")
    source_label = label_for_source(source_raw)

    # rooms / people breakdown (best effort)
    people = None; adults = children = infants = pets = None; key_code = None
    rooms = bk.get("rooms") or []
    if rooms:
        r0 = rooms[0]
        gb = r0.get("guest_breakdown") or {}
        adults  = gb.get("adults");   children = gb.get("children")
        infants = gb.get("infants");  pets     = gb.get("pets")
        people  = r0.get("people") or ((adults or 0) + (children or 0) + (infants or 0) + (pets or 0))
        key_code = r0.get("key_code") or ""

    # misc
    language = bk.get("language")
    thread_uid = bk.get("thread_uid")
    created_at = iso_date(bk.get("created_at"))
    updated_at = iso_date(bk.get("updated_at"))
    canceled_at = iso_date(bk.get("canceled_at"))

    # build column_values
    cv = {}
    put(cv, "reservation_id", res_id)
    put(cv, "unit", unit_name)
    put(cv, "property_id", str(property_id) if property_id else None)
    put(cv, "guest_name", display_name)

    # Email/Phone — სწორი სტრუქტურით
    if email:
        put(cv, "email", {"email": email, "text": email})
    if phone:
        put(cv, "phone", {"phone": phone})

    put(cv, "check_in", {"date": check_in})
    put(cv, "check_out", {"date": check_out})
    put(cv, "nights", nights)

    if source_label:
        put(cv, "source", {"labels": [source_label]})  # dropdown
    else:
        put(cv, "source_text", source_raw.strip() or None)

    put(cv, "status", {"label": status_label})         # status/color
    if stay_phase:
        put(cv, "stay_status", {"label": stay_phase})  # NEW stay status
    put(cv, "last_sync", {"date": today_iso()})

    put(cv, "currency", currency)
    put(cv, "total", total_amount)
    put(cv, "amount_paid", amount_paid)
    put(cv, "amount_due", amount_due)

    put(cv, "language", language)
    put(cv, "adults", adults)
    put(cv, "children", children)
    put(cv, "infants", infants)
    put(cv, "pets", pets)
    put(cv, "people", people)

    put(cv, "key_code", key_code)
    put(cv, "thread_uid", thread_uid)

    put(cv, "created_at", {"date": created_at})
    put(cv, "updated_at", {"date": updated_at})
    put(cv, "canceled_at", {"date": canceled_at})

    put(cv, "booking_status", (status_raw or "").strip())

    # raw JSON snapshot (compact)
    try:
        raw_compact = json.dumps(bk, separators=(",", ":"), ensure_ascii=False)[:50000]
        put(cv, "raw_json", raw_compact)
    except Exception:
        pass

    return {"item_name": display_name, "external_id": res_id, "column_values": cv}

# -----------------------
# Flask endpoints
# -----------------------
LODGY_API_BASE = os.getenv("LODGY_API_BASE", "https://api.lodgify.com")
LODGY_API_KEY  = os.getenv("LODGY_API_KEY", "")
MONDAY_API_BASE = os.getenv("MONDAY_API_BASE", "https://api.monday.com/v2")
MONDAY_API_KEY  = os.getenv("MONDAY_API_KEY", "")
try:
    MONDAY_BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "0"))
except Exception:
    MONDAY_BOARD_ID = 0

lodgify = LodgifyClient(api_base=LODGY_API_BASE, api_key=LODGY_API_KEY)
monday  = MondayClient(api_base=MONDAY_API_BASE, api_key=MONDAY_API_KEY, board_id=MONDAY_BOARD_ID)

@app.errorhandler(Exception)
def _unhandled(e):
    log.exception("Unhandled error")
    return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/health")
def health():
    ready = bool(LODGY_API_KEY and MONDAY_API_KEY and MONDAY_BOARD_ID)
    env = {
        "LODGY_API_BASE": LODGY_API_BASE,
        "LODGY_API_KEY_set": bool(LODGY_API_KEY),
        "MONDAY_API_BASE": MONDAY_API_BASE,
        "MONDAY_API_KEY_set": bool(MONDAY_API_KEY),
    }
    return jsonify({"ok": True, "service": "lodgify-monday", "board_id": MONDAY_BOARD_ID, "ready": ready, "env": env}), 200

@app.get("/")
def root():
    return jsonify({"ok": True, "endpoints": ["/health", "/lodgify-sync-all", "/webhook/lodgify", "/diag/monday-columns", "/diag/ping-lodgify", "/diag/ping-monday"]}), 200

@app.get("/diag/monday-columns")
def diag_monday_columns():
    try:
        query = """
        query($board_id: [ID!]) {
          boards(ids: $board_id) {
            id
            name
            columns { id title type }
          }
        }
        """
        data = monday._gql(query, {"board_id": [str(MONDAY_BOARD_ID)]})
        boards = (data or {}).get("boards") or []
        cols = boards[0]["columns"] if boards else []
        slim = [{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols]
        return jsonify({"ok": True, "board_id": MONDAY_BOARD_ID, "columns": slim}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/ping-lodgify")
def ping_lodgify():
    try:
        lodgify.list_bookings(limit=1, skip=0)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/ping-monday")
def ping_monday():
    try:
        q = "query { me { id name } }"
        data = monday._gql(q, {})
        return jsonify({"ok": True, "me": data.get("me")}), 200
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
    max_sec = int(request.args.get("max_sec", 20))  # prevent gunicorn timeout
    t0 = time.time()

    bookings = lodgify.list_bookings(limit=limit, skip=skip)
    results = []
    processed = 0
    next_skip = skip

    for bk in bookings:
        if time.time() - t0 > max_sec:
            break
        try:
            mapped = map_booking_to_monday(bk)
            res: UpsertResult = monday.upsert_item(mapped)
            results.append(res.to_dict())
            processed += 1
            next_skip += 1
        except Exception as e:
            log.exception("Upsert failed for booking id=%s", bk.get("id"))
            results.append({"ok": False, "error": str(e), "source_id": bk.get("id")})

    resp = {"ok": True, "count": len(results), "processed": processed, "next_skip": next_skip}
    if debug and bookings:
        resp["sample_input"] = bookings[:1]
        resp["sample_mapped"] = map_booking_to_monday(bookings[0])
    resp["results"] = results
    return jsonify(resp), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
