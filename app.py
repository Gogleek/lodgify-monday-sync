import os, json, logging, re, requests
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

# -----------------------
# Helpers
# -----------------------
E164_RE = re.compile(r"^\+?[1-9]\d{6,14}$")
ONLY_DIGITS_OR_PIPES = re.compile(r"^[\d| ]+$")

def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw).replace("(0)", "")
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

def today_date():
    return datetime.now(timezone.utc).date()

# -----------------------
# COLUMN MAP ( შენი IDs )
# -----------------------
COLUMN_MAP = {
    "reservation_id": "text_mkv47vb1",     # Booking ID — lookup
    "unit":           "text_mkv49eqm",     # Property/Unit
    "property_id":    "text_mkv4n35j",     # Property ID
    "guest_name":     "text_mkv46pev",
    "email":          "email_mkv4mbte",
    "phone":          "phone_mkv4yk8k",
    "check_in":       "date_mkv4npgx",
    "check_out":      "date_mkv46w1t",
    "nights":         "numeric_mkv4j5aq",
    "source":         "dropdown_mkv47kzc", # Booking.com / Airbnb / Expedia / Vrbo
    "status":         "color_mkv4zrs6",    # Completed / Confirmed / Paid / Pending / Cancelled
    "op_status":      "color_mkv4v5f0",    # Upcoming / In house / Completed (არ არის კრიტიკული; თუ არ არსებობს, ვიგნორებთ)
    "last_sync":      "date_mkv44erw",
    "raw_json":       "long_text_mkv4y19w",
    # "booking_status": "text_mkv4kjxs",   # ეს ბევრ ბორდზე არ გაქვს; აღარ ვგზავნით, თორემ მთელ რიქვესთს აგდებს
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
}

# ზუსტად შენი ლეიბლები
STATUS_LABELS_EXACT = {
    "confirmed": "Confirmed",
    "booked":    "Confirmed",
    "paid":      "Paid",
    "pending":   "Pending",
    "cancelled": "Cancelled",
    "canceled":  "Cancelled",
}
STATUS_DEFAULT = "Confirmed"

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

# -----------------------
# HTTP shim for debug
# -----------------------
_original_request = requests.sessions.Session.request
def _request(self, method, url, **kwargs):
    log.debug("[HTTP] %s %s", method, url)
    return _original_request(self, method, url, **kwargs)
requests.sessions.Session.request = _request

# -----------------------
# Lodgify Client
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
        self._rental_name_cache: Dict[str, str] = {}

    def list_bookings(self, limit: int = 50, skip: int = 0) -> List[dict]:
        url = f"{self.api_base}/v2/reservations/bookings"
        params = {"take": max(1, int(limit)), "skip": max(0, int(skip))}
        log.info("[Lodgify] GET %s params=%s", url, params)
        resp = self.session.get(url, params=params, timeout=45)

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
        log.info("[Lodgify] fetched %d items", len(items))
        return items

    def get_rental_name(self, rental_id: Optional[str]) -> Optional[str]:
        """სხვა ჩანაწერებზე რომვე გამოვიყენოთ, ქეშიც გვაქვს."""
        if not rental_id:
            return None
        rid = str(rental_id)
        if rid in self._rental_name_cache:
            return self._rental_name_cache[rid]
        # ვცდი რამდენიმე ვარიანტს; თუ ვერ ვნახე — ვაბრუნებ None-ს
        candidates = [
            f"{self.api_base}/v2/rentals/{rid}",
            f"{self.api_base}/v2/properties/{rid}",
        ]
        for url in candidates:
            try:
                r = self.session.get(url, timeout=20)
                if not r.ok:
                    continue
                data = r.json() or {}
                name = None
                if isinstance(data, dict):
                    name = data.get("name") or data.get("title")
                    if not name and "rental" in data and isinstance(data["rental"], dict):
                        name = data["rental"].get("name") or data["rental"].get("title")
                if name:
                    self._rental_name_cache[rid] = name
                    return name
            except Exception:
                continue
        return None

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
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        })
        self._column_ids: Optional[set] = None

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

    def _load_columns(self):
        if self._column_ids is not None:
            return
        q = """
        query($board_id: [ID!]) {
          boards(ids: $board_id) {
            columns { id }
          }
        }
        """
        data = self._gql(q, {"board_id": [str(self.board_id)]})
        boards = (data or {}).get("boards") or []
        cols = boards[0]["columns"] if boards else []
        self._column_ids = {c["id"] for c in cols}
        log.info("Monday board %s loaded %d columns", self.board_id, len(self._column_ids))

    def _filter_cols(self, column_values: Dict[str, object]) -> Dict[str, object]:
        self._load_columns()
        if not self._column_ids:
            return column_values
        # დატოვე მხოლოდ არსებული column_id-ები
        return {cid: val for cid, val in column_values.items() if cid in self._column_ids}

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
        cols = self._filter_cols(column_values)
        data = self._gql(query, {"board_id": str(self.board_id), "name": item_name, "cols": json.dumps(cols)})
        return int(data["create_item"]["id"])

    def update_item(self, item_id: int, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $item_id: ID!, $cols: JSON!) {
          change_multiple_column_values(board_id: $board_id, item_id: $item_id, column_values: $cols) { id }
        }
        """
        cols = self._filter_cols(column_values)
        data = self._gql(query, {"board_id": str(self.board_id), "item_id": str(item_id), "cols": json.dumps(cols)})
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
                if "missing_column" in str(inner_e) or "Column not found" in str(inner_e):
                    log.warning("Lookup column '%s' missing on board %s. Creating without lookup.", lookup_col, self.board_id)
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
BAD_RENTAL_NAMES = {"airbnbintegration", "direct after airbnb", "false", "false}"}
RENTAL_NAME_CACHE: Dict[str, str] = {}  # property_id -> name (process cache)

def label_for_source(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    r = raw.lower()
    for k, v in SOURCE_LABELS.items():
        if k in r:
            return v
    return None

def extract_unit_from_source_text(st: str) -> Optional[str]:
    if not st:
        return None
    # 1) ბოლო ფრჩხილები
    m = re.search(r"\(([^()]+)\)\s*$", st)
    if m:
        cand = m.group(1).strip()
        if cand and not ONLY_DIGITS_OR_PIPES.match(cand) and cand.lower() not in BAD_RENTAL_NAMES:
            return cand
    # 2) ბოლო დეფისის შემდეგი სიტყვები (მაგ: "... - B30")
    m = re.search(r"-\s*([A-Za-z][A-Za-z0-9' /\-]+)\s*$", st)
    if m:
        cand = m.group(1).strip()
        if cand and not ONLY_DIGITS_OR_PIPES.match(cand) and cand.lower() not in BAD_RENTAL_NAMES:
            return cand
    return None

def extract_unit_name(bk: dict) -> Optional[str]:
    # source_text
    st = (bk.get("source_text") or "").strip()
    cand = extract_unit_from_source_text(st)
    if cand:
        return cand

    # rental.name
    rname = ((bk.get("rental") or {}).get("name") or "").strip()
    if rname:
        low = rname.lower()
        if low not in BAD_RENTAL_NAMES and not ONLY_DIGITS_OR_PIPES.match(rname):
            return rname

    return None

def monday_main_status(bk: dict, check_in: Optional[str], check_out: Optional[str]) -> str:
    try:
        if check_out:
            co = datetime.strptime(check_out, "%Y-%m-%d").date()
            if co < today_date():
                return "Completed"
    except Exception:
        pass
    raw = (bk.get("status") or "").lower().strip()
    return STATUS_LABELS_EXACT.get(raw, STATUS_DEFAULT)

def monday_operational_status(check_in: Optional[str], check_out: Optional[str], cancelled: bool) -> Optional[str]:
    if not check_in or not check_out:
        return None
    try:
        ci = datetime.strptime(check_in, "%Y-%m-%d").date()
        co = datetime.strptime(check_out, "%Y-%m-%d").date()
        td = today_date()
        if cancelled:
            return "Completed" if co < td else None
        if td < ci:
            return "Upcoming"
        if ci <= td <= co:
            return "In house"
        if td > co:
            return "Completed"
    except Exception:
        return None
    return None

def map_booking_to_monday(bk: dict) -> dict:
    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "")
    property_id = bk.get("property_id") or (bk.get("rental") or {}).get("id")
    pid_str = str(property_id) if property_id else None

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
    email_raw = (guest.get("email") or "").strip() or None
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
    source_text = (bk.get("source_text") or "").strip()
    source_raw  = ((bk.get("source") or "") + (" " + source_text if source_text else "")).strip()
    source_label = label_for_source(source_raw)
    cancelled_flag = (status_raw or "").lower().strip() in ("cancelled", "canceled")

    # unit with multi-fallback and cross-cache
    unit_name = extract_unit_name(bk)
    if not unit_name and pid_str and pid_str in RENTAL_NAME_CACHE:
        unit_name = RENTAL_NAME_CACHE[pid_str]
    if not unit_name and pid_str:
        # ბოლო ფოლბექი — ვცდი Lodgify-დან წამოღებას /v2/rentals/{id}
        try:
            rn = lodgify.get_rental_name(pid_str)
            if rn and rn.lower() not in BAD_RENTAL_NAMES and not ONLY_DIGITS_OR_PIPES.match(rn):
                unit_name = rn
        except Exception:
            pass
    if unit_name and pid_str:
        RENTAL_NAME_CACHE[pid_str] = unit_name
    if not unit_name:
        unit_name = "Unknown unit"

    # rooms / people breakdown
    people = None; adults = children = infants = pets = None; key_code = None
    rooms = bk.get("rooms") or []
    if rooms:
        r0 = rooms[0]
        gb = r0.get("guest_breakdown") or {}
        adults  = gb.get("adults");   children = gb.get("children")
        infants = gb.get("infants");  pets     = gb.get("pets")
        try:
            ppl = 0
            for k in (adults, children, infants, pets):
                if isinstance(k, int):
                    ppl += k
            people = ppl if ppl > 0 else (r0.get("people") or None)
        except Exception:
            people = r0.get("people")
        key_code = r0.get("key_code") or ""

    # build column_values
    cv = {}
    put(cv, "reservation_id", res_id)
    put(cv, "unit", unit_name)
    put(cv, "property_id", pid_str)
    put(cv, "guest_name", display_name)

    if email_raw:
        put(cv, "email", {"email": email_raw, "text": email_raw})

    put(cv, "phone", phone or None)

    put(cv, "check_in", {"date": check_in} if check_in else None)
    put(cv, "check_out", {"date": check_out} if check_out else None)
    put(cv, "nights", nights)

    if source_label:
        put(cv, "source", {"labels": [source_label]})
    else:
        put(cv, "source_text", source_raw or None)

    main_status = monday_main_status(bk, check_in, check_out)
    put(cv, "status", {"label": main_status})

    op_col = COLUMN_MAP.get("op_status")
    if op_col:
        op_val = monday_operational_status(check_in, check_out, cancelled_flag)
        if op_val:
            cv[op_col] = {"label": op_val}

    put(cv, "last_sync", {"date": today_iso()})

    put(cv, "currency", currency)
    put(cv, "total", total_amount)
    put(cv, "amount_paid", amount_paid)
    put(cv, "amount_due", amount_due)

    put(cv, "language", bk.get("language"))
    put(cv, "adults", adults)
    put(cv, "children", children)
    put(cv, "infants", infants)
    put(cv, "pets", pets)
    put(cv, "people", people)

    put(cv, "key_code", key_code)
    put(cv, "thread_uid", bk.get("thread_uid"))

    put(cv, "created_at", {"date": iso_date(bk.get("created_at"))})
    put(cv, "updated_at", {"date": iso_date(bk.get("updated_at"))})
    put(cv, "canceled_at", {"date": iso_date(bk.get("canceled_at"))})

    # booking_status COL ამოღებულია — ბევრ ბორდზე არ არსებობს და მთელ რიქვესთს აგდებს

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

@app.get("/favicon.ico")
def favicon():
    return ("", 204)

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
    return jsonify({"ok": True, "endpoints": ["/health", "/lodgify-sync-all", "/webhook/lodgify", "/diag/monday-columns"]}), 200

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
    max_sec = int(request.args.get("max_sec", 20))

    started = datetime.now(timezone.utc)
    processed = 0
    results = []

    bookings = lodgify.list_bookings(limit=limit, skip=skip)

    for bk in bookings:
        try:
            mapped = map_booking_to_monday(bk)
            res: UpsertResult = monday.upsert_item(mapped)
            results.append(res.to_dict())
            processed += 1
        except Exception as e:
            log.exception("Upsert failed for booking id=%s", bk.get("id"))
            results.append({"ok": False, "error": str(e), "source_id": bk.get("id")})

        if (datetime.now(timezone.utc) - started).total_seconds() > max_sec:
            break

    next_skip = skip + processed if processed > 0 else skip
    resp = {"ok": True, "count": len(results), "processed": processed, "next_skip": next_skip, "results": results}
    if debug and bookings:
        resp["sample_input"] = bookings[:1]
        resp["sample_mapped"] = map_booking_to_monday(bookings[0])
    return jsonify(resp), 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
