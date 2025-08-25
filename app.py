import os, json, logging, re, requests, time
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
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

# -----------------------
# COLUMN MAP (შენი IDs)
# -----------------------
COLUMN_MAP = {
    "reservation_id": "text_mkv47vb1",     # Booking ID (Text) — lookup key
    "unit":           "text_mkv49eqm",     # Property / Unit (Text)
    "property_id":    "text_mkv4n35j",     # Property ID (Text)
    "guest_name":     "text_mkv46pev",     # Guest (Text)
    "email":          "email_mkv4mbte",    # Email (email column)
    "phone":          "phone_mkv4yk8k",    # Phone (phone column)
    "check_in":       "date_mkv4npgx",     # Check-in (Date)
    "check_out":      "date_mkv46w1t",     # Check-out (Date)
    "nights":         "numeric_mkv4j5aq",  # Nights (Numbers)
    "source":         "dropdown_mkv47kzc", # Source (Dropdown)
    "status":         "color_mkv4zrs6",    # Status (Status/color)
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

    # ახალი stay lifecycle/status column
    "stay_lifecycle": "color_mkv4v5f0",    # Upcoming / In house / Completed
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

# მხოლოდ ის ლეიბლები, რაც ბორდზე ნამდვილად გაქვს (Booking.com, Airbnb, Expedia, Vrbo)
SOURCE_LABELS = {
    "booking.com": "Booking.com",
    "airbnb":      "Airbnb",
    "expedia":     "Expedia",
    "vrbo":        "Vrbo",
}

def put(cv: dict, logical_key: str, value):
    col_id = COLUMN_MAP.get(logical_key)
    if col_id is not None and value is not None:
        cv[col_id] = value

# -----------------------
# Unit parsing / sanitize
# -----------------------
BAD_UNIT_TOKENS = {"false", "true", "null", "none", "nan"}

def sanitize_unit_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    s = str(name).strip()
    s = re.sub(r"[\{\}\[\]]", "", s)
    if len(s) < 2 or len(s) > 64:
        return None
    if not re.search(r"[A-Za-z0-9ა-ჰ]", s):
        return None
    if s.lower().strip("}") in BAD_UNIT_TOKENS:
        return None
    return s

def parse_unit_from_source_text(source_text: str) -> Optional[str]:
    if not source_text:
        return None
    st = source_text.strip()

    m = re.search(r"\(([^)]+)\)\s*$", st)     # (Queens 8)
    if m:
        c = sanitize_unit_name(m.group(1))
        if c: return c

    m = re.search(r"-\s*([^-\(\)]+)\s*$", st) # - Queens 8
    if m:
        c = sanitize_unit_name(m.group(1))
        if c: return c

    m = re.search(r":\s*([^:]+)\s*$", st)     # : Queens 8
    if m:
        c = sanitize_unit_name(m.group(1))
        if c: return c

    return None

def lifecycle_label(check_in: Optional[str], check_out: Optional[str], status_raw: str) -> str:
    """Maps to Monday color_mkv4v5f0 labels: Upcoming / In house / Completed"""
    today = datetime.now(timezone.utc).date()
    si = datetime.strptime(check_in, "%Y-%m-%d").date() if check_in else None
    so = datetime.strptime(check_out, "%Y-%m-%d").date() if check_out else None

    if (status_raw or "").lower() in ("cancelled", "canceled"):
        return "Completed"  # დასრულებულებად ვთვლით, რომ ფილტრში არ ჩერდეს აქტიურში

    if si and so:
        if si <= today < so:
            return "In house"
        elif today < si:
            return "Upcoming"
        else:
            return "Completed"
    if si:
        return "Upcoming" if today < si else "Completed"
    return "Upcoming"

def deduce_source(bk: dict) -> Tuple[Optional[str], Optional[str]]:
    """Return (dropdown_label_or_none, raw_text_or_none). If label is None -> dropdown will be cleared."""
    cand = []
    for k in ("source", "source_text", "ota", "origin", "channel"):
        v = bk.get(k)
        if isinstance(v, str) and v.strip():
            cand.append(v.strip())
    ext = bk.get("external_booking") or {}
    if isinstance(ext, dict):
        for k in ("source", "source_text", "channel", "origin", "ota", "provider"):
            v = ext.get(k)
            if isinstance(v, str) and v.strip():
                cand.append(v.strip())
    gemail = ((bk.get("guest") or {}).get("email") or "").lower()
    if "guest.booking.com" in gemail:
        cand.append("booking.com")

    raw = " ".join(cand).strip() or None
    raw_l = (raw or "").lower()
    for key, lbl in SOURCE_LABELS.items():
        if key in raw_l:
            return lbl, raw
    return None, raw

# -----------------------
# Lodgify Client (v2) + name resolvers
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
        self._property_cache: Dict[int, Optional[str]] = {}
        self._roomtype_cache: Dict[int, Optional[str]] = {}

    def list_bookings(self, limit: int = 50, skip: int = 0) -> List[dict]:
        url = f"{self.api_base}/v2/reservations/bookings"
        params = {"take": max(1, int(limit)), "skip": max(0, int(skip))}
        resp = self.session.get(url, params=params, timeout=45)

        # fallback: page/pageSize style
        if resp.status_code in (400, 404):
            page_size = max(1, int(limit))
            page_number = max(1, (int(skip) // page_size) + 1)
            params = {"pageSize": page_size, "pageNumber": page_number}
            resp = self.session.get(url, params=params, timeout=45)

        if not resp.ok:
            raise RuntimeError(f"Lodgify error {resp.status_code}: {resp.text[:500]}")

        data = resp.json() or {}
        items = data.get("results") or data.get("items") or data.get("data") or data
        if isinstance(items, dict):
            items = list(items.values())
        if not isinstance(items, list):
            items = []
        log.info("Lodgify fetched %d item(s) (limit=%s, skip=%s)", len(items), limit, skip)
        return items

    # --- resolvers ---
    def get_property_name(self, property_id: Optional[int]) -> Optional[str]:
        if not property_id:
            return None
        if property_id in self._property_cache:
            return self._property_cache[property_id]
        name = None
        for path in [
            f"/v2/rentals/{property_id}",
            f"/v1/rentals/{property_id}",
            f"/v2/properties/{property_id}",
        ]:
            try:
                r = self.session.get(self.api_base + path, timeout=30)
                if r.ok:
                    j = r.json() or {}
                    name = j.get("name") or j.get("title") or j.get("rentalName")
                    if name: break
            except Exception:
                pass
        name = sanitize_unit_name(name)
        self._property_cache[property_id] = name
        if name:
            log.info("Resolved property %s → %s", property_id, name)
        return name

    def get_roomtype_name(self, room_type_id: Optional[int], property_id: Optional[int]) -> Optional[str]:
        if not room_type_id:
            return None
        if room_type_id in self._roomtype_cache:
            return self._roomtype_cache[room_type_id]
        name = None
        for path in [
            f"/v2/rentals/{property_id}/roomtypes/{room_type_id}" if property_id else None,
            f"/v2/roomtypes/{room_type_id}",
            f"/v1/roomtypes/{room_type_id}",
        ]:
            if not path: 
                continue
            try:
                r = self.session.get(self.api_base + path, timeout=30)
                if r.ok:
                    j = r.json() or {}
                    name = j.get("name") or j.get("title")
                    if name: break
            except Exception:
                pass
        name = sanitize_unit_name(name)
        self._roomtype_cache[room_type_id] = name
        if name:
            log.info("Resolved roomtype %s → %s", room_type_id, name)
        return name

    def resolve_unit_name(self, property_id: Optional[int], room_type_id: Optional[int],
                          source_text: Optional[str], fallback: Optional[str]) -> Optional[str]:
        n1 = parse_unit_from_source_text(source_text or "")
        if n1: return n1
        n2 = self.get_roomtype_name(room_type_id, property_id)
        if n2: return n2
        n3 = self.get_property_name(property_id)
        if n3: return n3
        return sanitize_unit_name(fallback)

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
        r = self.session.post(self.api_base, data=json.dumps(payload), timeout=45)
        if r.status_code != 200:
            raise RuntimeError(f"Monday HTTP {r.status_code}: {r.text[:500]}")
        out = r.json()
        if "errors" in out:
            raise RuntimeError(f"Monday GQL error: {out['errors']}")
        return out.get("data", {})

    def find_item_by_external_id(self, column_id: str, external_id: str) -> Optional[int]:
        query = """
        query($board_id: ID!, $column_id: String!, $value: String!) {
          items_page_by_column_values(
            board_id: $board_id,
            columns: [{column_id: $column_id, column_values: [$value]}],
            limit: 1
          ) { items { id } }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "column_id": column_id, "value": external_id})
        items = (((data or {}).get("items_page_by_column_values") or {}).get("items")) or []
        return int(items[0]["id"]) if items else None

    def create_item(self, item_name: str, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $name: String!, $cols: JSON!) {
          create_item(board_id: $board_id, item_name: $name, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "name": item_name, "cols": json.dumps(column_values, ensure_ascii=False)})
        return int(data["create_item"]["id"])

    def update_item(self, item_id: int, column_values: Dict[str, object]) -> int:
        # ვერსიის შესაბამისად πλέον სჭირდება board_id
        query = """
        mutation($board_id: ID!, $item_id: ID!, $cols: JSON!) {
          change_multiple_column_values(item_id: $item_id, board_id: $board_id, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "item_id": str(item_id), "cols": json.dumps(column_values, ensure_ascii=False)})
        return int(data["change_multiple_column_values"]["id"])

    def upsert_item(self, mapped: dict) -> UpsertResult:
        item_name = mapped["item_name"]
        external_id = mapped["external_id"]
        column_values = mapped["column_values"]

        lookup_col = COLUMN_MAP["reservation_id"]
        try:
            existing_id = None
            try:
                existing_id = self.find_item_by_external_id(lookup_col, external_id)
            except Exception as inner_e:
                if "Column not found" in str(inner_e) or "missing_column" in str(inner_e):
                    log.warning("Lookup column '%s' not found on board %s. Creating without lookup.", lookup_col, self.board_id)
                else:
                    raise

            if existing_id:
                self.update_item(existing_id, column_values)
                log.info("Updated Monday item id=%s (ext=%s)", existing_id, external_id)
                return UpsertResult(ok=True, item_id=existing_id, created=False, updated=True)
            else:
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
def label_for_status(raw: str) -> str:
    key = (raw or "").lower().strip()
    return STATUS_LABELS.get(key, STATUS_DEFAULT)

def map_booking_to_monday(bk: dict) -> dict:
    # identifiers & meta
    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "")
    property_id = bk.get("property_id") or (bk.get("rental") or {}).get("id")

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
    email = guest.get("email") or ""
    phone = normalize_phone(guest.get("phone") or guest.get("mobile") or "")

    # dates
    check_in = iso_date(bk.get("arrival") or bk.get("check_in"))
    check_out = iso_date(bk.get("departure") or bk.get("check_out"))
    nights = days_between(check_in, check_out)

    # money
    total_amount = safe_float(bk.get("total_amount") or bk.get("total") or bk.get("price_total"))
    amount_paid = safe_float(bk.get("amount_paid"))
    amount_due  = safe_float(bk.get("amount_due"))
    currency = bk.get("currency_code") or bk.get("currency") or "GBP"

    # status / source
    status_raw = bk.get("status") or ""
    status_label = label_for_status(status_raw)
    source_text = (bk.get("source_text") or "").strip()
    src_label, src_raw_text = deduce_source(bk)

    # rooms / people breakdown (best effort)
    people = None; adults = children = infants = pets = None; key_code = None
    rooms = bk.get("rooms") or []
    if rooms:
        r0 = rooms[0]
        gb = r0.get("guest_breakdown") or {}
        adults  = gb.get("adults");   children = gb.get("children")
        infants = gb.get("infants");  pets     = gb.get("pets")
        try:
            people  = r0.get("people") or (adults or 0) + (children or 0) + (infants or 0) + (pets or 0)
        except Exception:
            people = None
        key_code = r0.get("key_code") or ""

    # UNIT (Property/Room name best-effort)
    fallback_unit = sanitize_unit_name((bk.get("rental") or {}).get("name") or bk.get("unit_name"))
    unit_name = sanitize_unit_name(
        parse_unit_from_source_text(source_text)
        or (bk.get("rental") or {}).get("name")
        or bk.get("unit_name")
    )
    room_type_id = rooms[0].get("room_type_id") if rooms else None
    if not unit_name:
        unit_name = lodgify.resolve_unit_name(property_id, room_type_id, source_text, fallback_unit)
    if not unit_name:
        unit_name = "Unknown unit"

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

    if email:
        put(cv, "email", {"email": email, "text": email})
    put(cv, "phone", phone if phone else None)

    put(cv, "check_in", {"date": check_in} if check_in else None)
    put(cv, "check_out", {"date": check_out} if check_out else None)
    put(cv, "nights", nights)

    # Source dropdown + raw text
    if src_label:
        put(cv, "source", {"labels": [src_label]})
    else:
        put(cv, "source", {"labels": []})  # გაასუფთავე dropdown
    if src_raw_text:
        put(cv, "source_text", src_raw_text)

    put(cv, "status", {"label": status_label})
    put(cv, "stay_lifecycle", {"label": lifecycle_label(check_in, check_out, status_raw)})
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

    put(cv, "created_at", {"date": created_at} if created_at else None)
    put(cv, "updated_at", {"date": updated_at} if updated_at else None)
    put(cv, "canceled_at", {"date": canceled_at} if canceled_at else None)

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
    return jsonify({
        "ok": True,
        "ready": bool(LODGY_API_KEY and MONDAY_API_KEY and MONDAY_BOARD_ID),
        "service": "lodgify-monday",
        "board_id": MONDAY_BOARD_ID,
        "env": {
            "LODGY_API_BASE": LODGY_API_BASE,
            "LODGY_API_KEY_set": bool(LODGY_API_KEY),
            "MONDAY_API_BASE": MONDAY_API_BASE,
            "MONDAY_API_KEY_set": bool(MONDAY_API_KEY),
        }
    }), 200

@app.get("/")
def root():
    return jsonify({"ok": True, "endpoints": [
        "/health", "/lodgify-sync-all", "/webhook/lodgify",
        "/diag/monday-columns", "/diag/ping-lodgify", "/diag/ping-monday"
    ]}), 200

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
def diag_ping_lodgify():
    try:
        items = lodgify.list_bookings(limit=1, skip=0)
        return jsonify({"ok": True, "seen": len(items)}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/ping-monday")
def diag_ping_monday():
    try:
        data = monday._gql("query { me { id name } }")
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
    max_sec = int(request.args.get("max_sec", "20"))  # worker timeout-safe batch

    start = time.time()
    results = []
    processed = 0

    while True:
        if time.time() - start > max_sec and processed > 0:
            break

        # Lodgify გვერდები მაინც 25-იანად მოდის; ჩვენ ვკითხვით შევსებული skip-ით
        bookings = lodgify.list_bookings(limit=limit, skip=skip)
        if not bookings:
            break

        for bk in bookings:
            if time.time() - start > max_sec:
                break
            try:
                mapped = map_booking_to_monday(bk)
                res: UpsertResult = monday.upsert_item(mapped)
                results.append(res.to_dict())
            except Exception as e:
                log.exception("Upsert failed for booking id=%s", bk.get("id"))
                results.append({"ok": False, "error": str(e), "source_id": bk.get("id")})
            processed += 1

        # Lodgify-ს რეალური გვერდის ზომა:
        page_size = len(bookings)
        if page_size == 0:
            break
        skip += page_size
        # თუ ნაკლებია, ბოლოს ვართ
        if page_size < limit and page_size < 25:
            break

        # batch-ებს შორის პატარა პაუზა anti-throttle
        time.sleep(0.2)

    resp = {"ok": True, "count": len(results), "processed": processed, "results": results, "next_skip": skip}
    if debug:
        # debug sample (პირველი ელემენტი და mapped მაგალითი)
        try:
            sample = lodgify.list_bookings(limit=1, skip=0)
            if sample:
                resp["sample_input"] = sample[:1]
                resp["sample_mapped"] = map_booking_to_monday(sample[0])
        except Exception:
            pass
    return jsonify(resp), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
