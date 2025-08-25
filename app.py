import os, json, logging, re, requests, uuid, time
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

# Instrument requests a bit for troubleshooting
_original_request = requests.Session.request
def _request(self, method, url, **kwargs):
    tag = None
    if "lodgify.com" in (url or ""):
        tag = f"Lodgify {uuid.uuid4().hex[:10]}"
    elif "monday.com" in (url or ""):
        tag = f"Monday {uuid.uuid4().hex[:10]}"
    if tag:
        log.info("[%s] %s %s params=%s", tag, method.upper(), url, kwargs.get("params"))
    return _original_request(self, method, url, **kwargs)
requests.Session.request = _request

# -----------------------
# Helpers
# -----------------------
E164_RE = re.compile(r"^\+?[1-9]\d{6,14}$")
PAREN_RE = re.compile(r"\(([^)]+)\)")

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
    if v is None or v == "":
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

def parse_date(s: Optional[str]):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)

def today_iso():
    return datetime.now(timezone.utc).date().isoformat()

def days_between(a: Optional[str], b: Optional[str]) -> Optional[int]:
    da = parse_date(a); db = parse_date(b)
    if not da or not db:
        return None
    return (db - da).days

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

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
    "status":         "color_mkv4zrs6",    # Booking Status (Status/color) - Confirmed/Paid/Pending/Cancelled
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

# Allowed dropdown labels (fallbacks if env not set)
DEFAULT_ALLOWED_SOURCE_LABELS = {"Booking.com", "Airbnb", "Expedia", "Vrbo", "Direct"}
env_labels = {s.strip() for s in os.getenv("SOURCE_LABELS_ALLOWED", "").split(",") if s.strip()}
ALLOWED_SOURCE_LABELS = env_labels or DEFAULT_ALLOWED_SOURCE_LABELS

# Booking-status label map
STATUS_LABELS = {
    "confirmed": "Confirmed",
    "booked":    "Confirmed",
    "paid":      "Paid",
    "pending":   "Pending",
    "cancelled": "Cancelled",
    "canceled":  "Cancelled",
}
STATUS_DEFAULT = "Pending"

def put(cv: dict, logical_key: str, value):
    col_id = COLUMN_MAP.get(logical_key)
    if col_id is not None and value is not None:
        cv[col_id] = value

# -----------------------
# Source mapping (robust)
# -----------------------
def label_for_source(raw_source: Optional[str], raw_source_text: Optional[str]) -> Optional[str]:
    blob = f"{_norm(raw_source)} {_norm(raw_source_text)}"
    if "booking.com" in blob or "booking com" in blob or "bookingcom" in blob:
        return "Booking.com"
    if "airbnb" in blob:
        return "Airbnb"
    if "expedia" in blob:
        return "Expedia"
    if "vrbo" in blob:
        return "Vrbo"
    if "direct" in blob:
        return "Direct"
    # არ ვაბრუნებთ "Manual"-ს — ბევრ ბორდზე ასეთი ლეიბლი საერთოდ არ არსებობს
    return None

# -----------------------
# Unit extractor
# -----------------------
def extract_unit_name(bk: dict) -> Optional[str]:
    # 1) rental.name
    rental = bk.get("rental") or {}
    name = (rental.get("name") or "").strip()
    if name:
        return name

    # 2) unit_name (თუ გაქვს სხვა ინტეგრაციიდან)
    if bk.get("unit_name"):
        u = str(bk["unit_name"]).strip()
        if u:
            return u

    # 3) source_text/source → ბოლო ფრჩხილები
    st = (bk.get("source_text") or bk.get("source") or "").strip()
    if st:
        matches = PAREN_RE.findall(st)
        if matches:
            candidate = matches[-1].strip()
            # იგნორირებულია სუფთა ციფრები/კოდები
            if re.search(r"[A-Za-z]", candidate) and not re.fullmatch(r"\d{5,}", candidate):
                return candidate

        # 4) დეფისის მარჯვენა ნაწილი — ბოლო 2-3 სიტყვა
        parts = [p.strip() for p in st.split("-") if p.strip()]
        if len(parts) >= 2:
            tail = parts[-1]
            tokens = [t for t in re.split(r"\s+", tail) if re.search(r"[A-Za-z]", t)]
            if tokens:
                guess = " ".join(tokens[-3:]).strip()
                if 2 <= len(guess) <= 40:
                    return guess

    # 5) rooms[0].key_code (ხშირად ცარიელია, მაგრამ ვცადოთ)
    rooms = bk.get("rooms") or []
    if rooms:
        kc = (rooms[0].get("key_code") or "").strip()
        if kc:
            return kc

    return None

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

    def find_item_by_external_id(self, column_id: str, external_id: str) -> Optional[Tuple[int, str]]:
        # ვცდილობთ active + archived
        query = """
        query($board_id: ID!, $column_id: String!, $value: String!) {
          items_page_by_column_values(
            board_id: $board_id,
            columns: [{column_id: $column_id, column_values: [$value]}],
            limit: 2
          ) { items { id state } }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "column_id": column_id, "value": external_id})
        items = (((data or {}).get("items_page_by_column_values") or {}).get("items")) or []
        if not items:
            return None
        # პრიორიტეტი active→archived
        active = [it for it in items if (it.get("state") or "").lower() == "active"]
        first = (active or items)[0]
        return (int(first["id"]), str(first.get("state") or "active"))

    def restore_item(self, item_id: int):
        query = "mutation($item_id: ID!) { restore_item(item_id: $item_id) { id } }"
        self._gql(query, {"item_id": str(item_id)})

    def create_item(self, item_name: str, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $name: String!, $cols: JSON!) {
          create_item(board_id: $board_id, item_name: $name, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "name": item_name, "cols": json.dumps(column_values, ensure_ascii=False)})
        return int(data["create_item"]["id"])

    def update_item(self, item_id: int, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $item_id: ID!, $cols: JSON!) {
          change_multiple_column_values(item_id: $item_id, board_id: $board_id, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "item_id": str(item_id), "cols": json.dumps(column_values, ensure_ascii=False)})
        return int(data["change_multiple_column_values"]["id"])

    def map_reservation_ids(self, page_limit: int = 200) -> tuple[int, Dict[str, List[int]]]:
        """დაასკანერე ბორდის ყველა აითემი და ააგროვე reservation_id => [item_ids]"""
        col_id = COLUMN_MAP["reservation_id"]

        q_first = """
        query($board_id: ID!, $limit: Int!, $col_ids: [String!]) {
          items_page(board_id: $board_id, limit: $limit) {
            cursor
            items { id state column_values(ids: $col_ids) { id text } }
          }
        }"""

        q_next = """
        query($cursor: String!, $col_ids: [String!]) {
          next_items_page(cursor: $cursor) {
            cursor
            items { id state column_values(ids: $col_ids) { id text } }
          }
        }"""

        res_map: Dict[str, List[int]] = {}
        total = 0

        data = self._gql(q_first, {"board_id": str(self.board_id), "limit": page_limit, "col_ids": [col_id]})
        page = (data or {}).get("items_page") or {}
        while True:
            items = page.get("items") or []
            for it in items:
                total += 1
                if (it.get("state") or "").lower() == "deleted":
                    continue
                rid = None
                for cv in (it.get("column_values") or []):
                    if cv.get("id") == col_id:
                        rid = (cv.get("text") or "").strip()
                        break
                if rid:
                    res_map.setdefault(rid, []).append(int(it["id"]))

            cursor = page.get("cursor")
            if not cursor:
                break
            data = self._gql(q_next, {"cursor": cursor, "col_ids": [col_id]})
            page = (data or {}).get("next_items_page") or {}

        return total, res_map

# -----------------------
# Mapping Lodgify → Monday
# -----------------------
def label_for_status(raw: str) -> str:
    key = (raw or "").lower().strip()
    return STATUS_LABELS.get(key, STATUS_DEFAULT)

def compute_stay_status_label(check_in: Optional[str], check_out: Optional[str]) -> Optional[str]:
    """Upcoming (future), In house (today within stay), Completed (past)."""
    today = parse_date(today_iso())
    ci = parse_date(check_in); co = parse_date(check_out)
    if not ci or not co or not today:
        return None
    if today < ci:
        return "Upcoming"
    if ci <= today < co:
        return "In house"
    return "Completed"

def map_booking_to_monday(bk: dict) -> dict:
    # identifiers & meta
    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "").strip()
    property_id = bk.get("property_id") or (bk.get("rental") or {}).get("id")
    if isinstance(property_id, bool):
        property_id = None
    unit_name = extract_unit_name(bk) or "Unknown unit"

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
    email = (guest.get("email") or "").strip() or None
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
    source_text = bk.get("source_text") or ""
    source_raw  = (bk.get("source") or "")
    source_label = label_for_source(source_raw, source_text)

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
    # Email column strict: value must be {"email": "...", "text": "..."}; text never None
    if email:
        put(cv, "email", {"email": email, "text": display_name or email})
    # Phone – მარტივად ჩავწეროთ ტექსტად (შენს ბორდს ასე ეჭირა OK)
    put(cv, "phone", phone or None)

    put(cv, "check_in", {"date": check_in})
    put(cv, "check_out", {"date": check_out})
    put(cv, "nights", nights)

    # Source dropdown – ვწერთ მხოლოდ თუ ლეიბლი ნებადართულია, თორემ ვაგდებთ source_text-ში
    if source_label and source_label in ALLOWED_SOURCE_LABELS:
        put(cv, "source", {"labels": [source_label]})  # dropdown
    else:
        # fallback – შევინახოთ სრული ტექსტი long_text-ში
        st_blob = (source_raw + (" " + source_text if source_text else "")).strip()
        put(cv, "source_text", st_blob or None)

    put(cv, "status", {"label": status_label})         # booking status/color
    put(cv, "last_sync", {"date": today_iso()})
    # Stay status (Upcoming / In house / Completed)
    stay_label = compute_stay_status_label(check_in, check_out)
    if stay_label:
        put(cv, "stay_status", {"label": stay_label})

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
    env = {
        "LODGY_API_BASE": LODGY_API_BASE,
        "LODGY_API_KEY_set": bool(LODGY_API_KEY),
        "MONDAY_API_BASE": MONDAY_API_BASE,
        "MONDAY_API_KEY_set": bool(MONDAY_API_KEY),
    }
    ready = bool(LODGY_API_KEY and MONDAY_API_KEY and MONDAY_BOARD_ID)
    return jsonify({"ok": True, "service": "lodgify-monday", "board_id": MONDAY_BOARD_ID, "env": env, "ready": ready}), 200

@app.get("/")
def root():
    return jsonify({"ok": True, "endpoints": [
        "/health",
        "/diag/ping-lodgify",
        "/diag/ping-monday",
        "/diag/monday-columns",
        "/diag/monday-unique-bookings",
        "/diag/lodgify-count",
        "/lodgify-sync-all"
    ]}), 200

@app.get("/diag/ping-lodgify")
def diag_ping_lodgify():
    items = lodgify.list_bookings(limit=1, skip=0)
    return jsonify({"ok": True, "count": len(items)}), 200

@app.get("/diag/ping-monday")
def diag_ping_monday():
    q = "query { me { id name } }"
    data = monday._gql(q, {})
    return jsonify({"ok": True, "me": (data or {}).get("me")}), 200

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

@app.get("/diag/monday-unique-bookings")
def diag_monday_unique_bookings():
    try:
        total, res_map = monday.map_reservation_ids(page_limit=200)
        unique_cnt = len(res_map)
        dups = {rid: ids for rid, ids in res_map.items() if len(ids) > 1}
        return jsonify({
            "ok": True,
            "board_id": MONDAY_BOARD_ID,
            "scanned_items": total,
            "unique_reservations": unique_cnt,
            "duplicate_count": len(dups),
            "duplicates": [{"reservation_id": k, "item_ids": v} for k, v in dups.items()]
        }), 200
    except Exception as e:
        log.exception("diag_monday_unique_bookings failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/lodgify-count")
def diag_lodgify_count():
    """Lodgify-ის უნიკალური ბუქინგების რაოდენობა (პაგინაციით). პარამეტრები: take, max_pages."""
    try:
        take = int(request.args.get("take", 50))
        max_pages = int(request.args.get("max_pages", 200))
        seen = set()
        skip = 0
        pages = 0
        total_seen = 0

        while pages < max_pages:
            items = lodgify.list_bookings(limit=take, skip=skip)
            if not items:
                break
            for bk in items:
                rid = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "").strip()
                if rid:
                    seen.add(rid)
                total_seen += 1
            # პრაქტიკაში Lodgify 25-ს აბრუნებს გვერდზე
            if len(items) < 1:
                break
            skip += len(items)
            pages += 1

        return jsonify({
            "ok": True,
            "pages_scanned": pages,
            "total_records_seen": total_seen,
            "unique_reservations": len(seen),
            "next_skip_hint": skip
        }), 200
    except Exception as e:
        log.exception("diag_lodgify_count failed")
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

# ---- upsert core ----
def _safe_upsert(mapped: dict) -> UpsertResult:
    item_name = mapped["item_name"]
    external_id = mapped["external_id"]
    column_values = mapped["column_values"]

    lookup_col = COLUMN_MAP["reservation_id"]
    try:
        found = monday.find_item_by_external_id(lookup_col, external_id)
        if found:
            item_id, state = found
            # თუ archived/ასევე active, ერთხელ ვცადოთ restore, მერე update
            if (state or "").lower() == "archived":
                try:
                    monday.restore_item(item_id)
                    log.info("Restored archived item id=%s (ext=%s)", item_id, external_id)
                except Exception as rexc:
                    log.warning("Restore failed id=%s: %s", item_id, rexc)
            monday.update_item(item_id, column_values)
            log.info("Updated Monday item id=%s (ext=%s)", item_id, external_id)
            return UpsertResult(ok=True, item_id=item_id, created=False, updated=True)
        else:
            safe_name = f"{item_name} • #{external_id}"
            new_id = monday.create_item(safe_name, column_values)
            log.info("Created Monday item id=%s (ext=%s)", new_id, external_id)
            return UpsertResult(ok=True, item_id=new_id, created=True, updated=False)
    except Exception as e:
        log.exception("Upsert failed for external_id=%s", external_id)
        return UpsertResult(ok=False, error=str(e))

MondayClient.upsert_item = staticmethod(_safe_upsert)  # monkey-attach

@app.get("/lodgify-sync-all")
def lodgify_sync_all():
    limit = int(request.args.get("limit", 50))
    skip = int(request.args.get("skip", 0))
    debug = request.args.get("debug", "0") == "1"
    max_sec = float(request.args.get("max_sec", 24))  # ~gunicorn 30sამდე

    t0 = time.time()
    bookings = lodgify.list_bookings(limit=limit, skip=skip)
    results = []
    processed = 0
    for bk in bookings:
        try:
            mapped = map_booking_to_monday(bk)
            res: UpsertResult = _safe_upsert(mapped)
            results.append(res.to_dict())
            processed += 1
        except Exception as e:
            log.exception("Upsert failed for booking id=%s", bk.get("id"))
            results.append({"ok": False, "error": str(e), "source_id": bk.get("id")})
        if (time.time() - t0) > max_sec:
            log.warning("Cut by max_sec=%.1f after processed=%d", max_sec, processed)
            break

    resp = {
        "ok": True,
        "count": len(results),
        "processed": processed,
        "next_skip": skip + processed
    }
    if debug and bookings:
        resp["sample_input"] = bookings[:1]
        resp["sample_mapped"] = map_booking_to_monday(bookings[0])
    return jsonify(resp), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
