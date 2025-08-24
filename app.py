# app.py
from flask import Flask, request, jsonify
import os, json, re, traceback
from datetime import datetime, date
import requests

app = Flask(__name__)

# ========= ENV =========
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

if not MONDAY_API_TOKEN or not LODGY_API_KEY:
    raise RuntimeError("Set MONDAY_API_TOKEN and LODGY_API_KEY env vars.")

MONDAY_HEADERS = {"Authorization": f"Bearer {MONDAY_API_TOKEN}", "Content-Type": "application/json"}
LODGY_HEADERS = {"X-ApiKey": LODGY_API_KEY}

# ========= CACHES =========
_COLUMN_CACHE = None  # list of columns with settings
_COLUMN_MAP = None    # our normalized mapping title->id
_PROPS_BY_ID = None
_PROPS_BY_NAME = None

# ========= CONSTANTS =========
DEFAULT_SOURCE_LABELS = ["Booking.com", "Airbnb", "Expedia", "Vrbo", "Manual", "Direct"]

SOURCE_NORMALIZE_MAP = {
    "bookingcom": "Booking.com",
    "booking.com": "Booking.com",
    "airbnb": "Airbnb",
    "expedia": "Expedia",
    "vrbo": "Vrbo",
    "homeaway": "Vrbo",
    "manual": "Manual",
    "direct": "Direct",
}

# Column Titles we expect on the board
EXPECTED_TITLES = {
    "Booking ID": "booking_id",
    "Property": "property",
    "Property ID": "property_id",
    "Guest": "guest",
    "Email": "email",
    "Phone": "phone",
    "Check-in": "check_in",
    "Check-out": "check_out",
    "Nights": "nights",
    "Source": "source",
    "Status": "status",
    "Last Sync": "last_sync",
    "Raw JSON": "raw_json",

    "Booking Status": "booking_status",
    "Currency": "currency",
    "Total Amount": "total_amount",
    "Amount Paid": "amount_paid",
    "Amount Due": "amount_due",
    "Source Text": "source_text",
    "Language": "language",
    "Adults": "adults",
    "Children": "children",
    "Infants": "infants",
    "Pets": "pets",
    "People": "people",
    "Key Code": "key_code",
    "Thread UID": "thread_uid",
    "Created At": "created_at",
    "Updated At": "updated_at",
    "Canceled At": "canceled_at",
}

# ========== UTILS ==========
def monday_graphql(query: str, variables: dict | None = None):
    body = {"query": query, "variables": variables or {}}
    r = requests.post("https://api.monday.com/v2", headers=MONDAY_HEADERS, json=body, timeout=40)
    try:
        return r.json()
    except Exception:
        return {"status": r.status_code, "text": r.text[:500]}

def lodgify_get(url: str, timeout: int = 40):
    return requests.get(url, headers=LODGY_HEADERS, timeout=timeout)

def parse_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def only_date(s: str | None) -> str:
    return s[:10] if s else ""

def days_between(a: str | None, b: str | None) -> str:
    da, db = parse_date(a), parse_date(b)
    if da and db:
        return str((db - da).days)
    return ""

def normalize_source_label(raw: str | None) -> str:
    if not raw:
        return ""
    key = re.sub(r"[^a-z0-9]+", "", str(raw).lower())
    return SOURCE_NORMALIZE_MAP.get(key, raw.strip())

def derive_source(b: dict) -> str:
    src = b.get("source") or b.get("channel") or ""
    src = normalize_source_label(src)
    if not src:
        em = ((b.get("guest") or {}).get("email") or "").lower()
        if "guest.booking.com" in em or "booking.com" in em:
            src = "Booking.com"
        elif "airbnb.com" in em:
            src = "Airbnb"
        elif "expedia" in em:
            src = "Expedia"
        elif "vrbo" in em or "homeaway" in em:
            src = "Vrbo"
    return normalize_source_label(src)

def compute_status_label(check_in: str | None, check_out: str | None) -> str:
    today = date.today()
    di, do = parse_date(check_in), parse_date(check_out)
    if di and today < di:
        return "Upcoming"
    if di and do and di <= today <= do:
        return "In house"
    if do and today > do:
        return "Completed"
    return ""

# ========== MONDAY COLUMNS ==========
def get_board_columns(include_settings: bool = True) -> list[dict]:
    global _COLUMN_CACHE
    if _COLUMN_CACHE is not None:
        return _COLUMN_CACHE
    q = """
    query($board: [ID!]!) {
      boards(ids: $board) {
        columns {
          id
          title
          type
          settings_str
        }
      }
    }
    """
    resp = monday_graphql(q, {"board": BOARD_ID})
    cols = (resp.get("data", {}) or {}).get("boards", [{}])[0].get("columns", []) if isinstance(resp.get("data"), dict) else []
    _COLUMN_CACHE = cols
    return cols

def build_column_map() -> dict:
    """
    Return mapping from our logical keys -> monday column id, using board titles.
    """
    global _COLUMN_MAP
    if _COLUMN_MAP is not None:
        return _COLUMN_MAP
    cols = get_board_columns(include_settings=True)
    title_to_id = {c["title"]: c["id"] for c in cols}
    m = {}
    for title, key in EXPECTED_TITLES.items():
        cid = title_to_id.get(title)
        if cid:
            m[key] = cid
    _COLUMN_MAP = m
    return m

def parse_settings_labels(settings_str: str | None) -> list[str]:
    """
    Extract label names regardless of shape:
      - {"labels":{"0":"A","1":"B"}}
      - {"labels":[{"id":1,"name":"A"}]}
      - {"labels":["A","B"]}
    """
    if not settings_str:
        return []
    try:
        data = json.loads(settings_str)
    except Exception:
        return []
    labels = data.get("labels")
    if isinstance(labels, dict):
        try:
            return [v for k, v in sorted(labels.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 1_000_000)]
        except Exception:
            return list(labels.values())
    if isinstance(labels, list):
        out = []
        for el in labels:
            if isinstance(el, str):
                out.append(el)
            elif isinstance(el, dict):
                name = el.get("name") or el.get("label") or el.get("title")
                if name:
                    out.append(name)
        return out
    return []

def ensure_dropdown_label_exists(column_key: str, wanted: str):
    """
    If dropdown label is missing, extend labels while preserving shape.
    """
    if not wanted:
        return
    cmap = build_column_map()
    col_id = cmap.get(column_key)
    if not col_id:
        return

    cols = get_board_columns(include_settings=True)
    col = next((c for c in cols if c["id"] == col_id), None)
    data = {}
    if col and col.get("settings_str"):
        try:
            data = json.loads(col["settings_str"])
        except Exception:
            data = {}

    labels = data.get("labels")
    existing_names = []
    shape = "none"

    if isinstance(labels, dict):
        existing_names = parse_settings_labels(col.get("settings_str"))
        shape = "map"
    elif isinstance(labels, list):
        shape = "list-objects" if any(isinstance(x, dict) for x in labels) else "list-strings"
        if shape == "list-objects":
            existing_names = [x.get("name") for x in labels if isinstance(x, dict) and x.get("name")]
        else:
            existing_names = [str(x) for x in labels if isinstance(x, (str, int, float))]
    else:
        existing_names = []

    merged = []
    for x in (existing_names + DEFAULT_SOURCE_LABELS + [wanted]):
        if x and x not in merged:
            merged.append(x)

    if merged == existing_names:
        return

    if shape == "list-objects":
        current = labels if isinstance(labels, list) else []
        max_id = 0
        for el in current:
            if isinstance(el, dict) and isinstance(el.get("id"), int):
                max_id = max(max_id, el["id"])
        by_name = {el.get("name"): el for el in current if isinstance(el, dict) and el.get("name")}
        new_list = []
        for name in merged:
            if name in by_name:
                new_list.append(by_name[name])
            else:
                max_id += 1
                new_list.append({"id": max_id, "name": name})
        data["labels"] = new_list
    elif shape == "list-strings":
        data["labels"] = merged
    else:
        data["labels"] = merged

    settings_str = json.dumps(data, ensure_ascii=False)
    q = """
    mutation($board: ID!, $column_id: ID!, $settings_str: String!) {
      change_column_settings(board_id: $board, column_id: $column_id, settings_str: $settings_str) { id }
    }
    """
    monday_graphql(q, {"board": BOARD_ID, "column_id": col_id, "settings_str": settings_str})
    # bust cache
    global _COLUMN_CACHE, _COLUMN_MAP
    _COLUMN_CACHE = None
    _COLUMN_MAP = None

# ========== LODGIFY ==========
def fetch_lodgify_properties_index():
    global _PROPS_BY_ID, _PROPS_BY_NAME
    if _PROPS_BY_ID is not None and _PROPS_BY_NAME is not None:
        return _PROPS_BY_ID, _PROPS_BY_NAME

    url = "https://api.lodgify.com/v1/properties"
    id_map, name_map = {}, {}
    try:
        r = lodgify_get(url)
        if r.status_code == 200:
            for p in r.json():
                pid = p.get("id")
                nm = (p.get("name") or "").strip()
                if pid is not None:
                    id_map[str(pid)] = nm
                if nm:
                    name_map[nm] = str(pid) if pid is not None else ""
        else:
            app.logger.warning(f"[Lodgify] properties {r.status_code}: {r.text[:200]}")
    except Exception as e:
        app.logger.warning(f"[Lodgify] properties fetch fail: {e}")

    _PROPS_BY_ID, _PROPS_BY_NAME = id_map, name_map
    return _PROPS_BY_ID, _PROPS_BY_NAME

def fetch_lodgify_bookings() -> list[dict]:
    url = "https://api.lodgify.com/v2/reservations/bookings"
    r = lodgify_get(url)
    if r.status_code != 200:
        app.logger.error(f"[Lodgify] {r.status_code} {r.text[:300]}")
        return []
    try:
        data = r.json()
    except Exception as e:
        app.logger.exception(f"[Lodgify] JSON parse error: {e}")
        return []
    return data.get("items", []) or []

# ========== FIELD EXTRACT ==========
def extract_fields(b: dict) -> dict:
    # core
    booking_id = str(b.get("id") or b.get("bookingId") or "")
    check_in = b.get("arrival") or b.get("check_in_date") or ""
    check_out = b.get("departure") or b.get("check_out_date") or ""
    nights = days_between(check_in, check_out)

    # guest
    g = b.get("guest") or {}
    guest_name = g.get("name") or "N/A"
    guest_email = g.get("email") or ""
    guest_phone = g.get("phone") or ""
    country = (g.get("country_code") or "").upper() if g.get("country_code") else ""

    # property
    prop_id = b.get("property_id")
    prop_name = ""
    p = b.get("property") if isinstance(b.get("property"), dict) else None
    if p:
        prop_id = prop_id or p.get("id")
        prop_name = p.get("name") or ""
    if prop_id:
        id_map, _ = fetch_lodgify_properties_index()
        prop_name = prop_name or id_map.get(str(prop_id), "")
    prop_id = str(prop_id) if prop_id else ""

    # rooms breakdown
    rooms = b.get("rooms") or []
    adults = children = infants = pets = people = 0
    key_code = ""
    for r in rooms:
        gb = (r.get("guest_breakdown") or {})
        adults   += int(gb.get("adults")   or 0)
        children += int(gb.get("children") or 0)
        infants  += int(gb.get("infants")  or 0)
        pets     += int(gb.get("pets")     or 0)
        people   += int(r.get("people")    or 0)
        if not key_code and r.get("key_code"):
            key_code = r.get("key_code")

    # source, status
    source = derive_source(b)
    status_label = compute_status_label(check_in, check_out)

    # money
    currency = b.get("currency_code") or ""
    total_amount = b.get("total_amount")
    amount_paid = b.get("amount_paid")
    amount_due = b.get("amount_due")

    # misc
    raw_json = json.dumps(b, ensure_ascii=False)
    if len(raw_json) > 15000:
        raw_json = raw_json[:15000]

    created_at = only_date(b.get("created_at"))
    updated_at = only_date(b.get("updated_at"))
    canceled_at = only_date(b.get("canceled_at"))

    fields = {
        "booking_id": booking_id,
        "guest": guest_name,
        "email": guest_email,
        "phone": guest_phone,
        "phone_country": country,
        "check_in": check_in,
        "check_out": check_out,
        "nights": nights,
        "property": prop_name,
        "property_id": prop_id,
        "source": source,
        "status_label": status_label,
        "last_sync": date.today().strftime("%Y-%m-%d"),
        "raw_json": raw_json,

        "booking_status": b.get("status") or "",
        "currency": currency,
        "total_amount": str(total_amount) if total_amount not in (None, "") else "",
        "amount_paid": str(amount_paid) if amount_paid not in (None, "") else "",
        "amount_due": str(amount_due) if amount_due not in (None, "") else "",
        "source_text": b.get("source_text") or "",
        "language": b.get("language") or "",
        "adults": str(adults),
        "children": str(children),
        "infants": str(infants),
        "pets": str(pets),
        "people": str(people),
        "key_code": key_code or "",
        "thread_uid": b.get("thread_uid") or "",
        "created_at": created_at,
        "updated_at": updated_at,
        "canceled_at": canceled_at,
    }
    return fields

# ========== MONDAY UPSERT ==========
def find_item_by_booking_id(booking_id: str) -> str | None:
    """
    Scan first 200 items and try to match Booking ID column text == booking_id.
    """
    cmap = build_column_map()
    bid_col = cmap.get("booking_id")
    if not bid_col:
        return None
    cursor = None
    fetched = 0
    while fetched < 200:
        q = """
        query($board: [ID!]!, $cursor: String) {
          boards(ids: $board) {
            items_page(limit: 50, cursor: $cursor) {
              cursor
              items {
                id
                name
                column_values { id text }
              }
            }
          }
        }
        """
        resp = monday_graphql(q, {"board": BOARD_ID, "cursor": cursor})
        page = (((resp.get("data") or {}).get("boards") or [{}])[0].get("items_page") or {})
        items = page.get("items") or []
        for it in items:
            for cv in it.get("column_values") or []:
                if cv.get("id") == bid_col and (cv.get("text") or "") == booking_id:
                    return it.get("id")
        cursor = page.get("cursor")
        if not items or not cursor:
            break
        fetched += len(items)
    return None

def build_column_values(fields: dict) -> dict:
    """
    Map fields to Monday column_values schema.
    """
    m = build_column_map()
    out = {}

    def set_text(key, val):
        cid = m.get(key)
        if not cid:
            return
        out[cid] = str(val) if val is not None else ""

    def set_date(key, val):
        cid = m.get(key)
        if not cid or not val:
            return
        out[cid] = {"date": val}

    def set_number(key, val):
        cid = m.get(key)
        if not cid:
            return
        out[cid] = str(val) if val not in (None, "") else ""

    def set_email(key, email, name=""):
        cid = m.get(key)
        if not cid or not email:
            return
        out[cid] = {"email": email, "text": name or email}

    def set_phone(key, phone, country_short=""):
        cid = m.get(key)
        if not cid or not phone:
            return
        obj = {"phone": phone}
        if country_short:
            obj["countryShortName"] = country_short
        out[cid] = obj

    def set_dropdown_labels(key, labels: list[str]):
        cid = m.get(key)
        if not cid or not labels:
            return
        out[cid] = {"labels": labels}

    def set_status_label(key, label: str):
        cid = m.get(key)
        if not cid or not label:
            return
        out[cid] = {"label": label}

    def set_long_text(key, txt: str):
        cid = m.get(key)
        if not cid or txt is None:
            return
        out[cid] = {"text": str(txt)}

    # map basics
    set_text("booking_id", fields.get("booking_id"))
    set_text("guest", fields.get("guest"))
    set_email("email", fields.get("email") or "", fields.get("guest") or "")
    set_phone("phone", fields.get("phone") or "", fields.get("phone_country") or "")
    set_text("property", fields.get("property") or "")
    set_text("property_id", fields.get("property_id") or "")
    set_date("check_in", fields.get("check_in") or "")
    set_date("check_out", fields.get("check_out") or "")
    set_number("nights", fields.get("nights") or "")
    # dropdown
    src = fields.get("source") or ""
    if src:
        ensure_dropdown_label_exists("source", src)
        set_dropdown_labels("source", [src])
    # status column
    set_status_label("status", fields.get("status_label") or "")
    # last sync / raw json
    set_date("last_sync", fields.get("last_sync") or "")
    set_long_text("raw_json", fields.get("raw_json") or "")

    # extras
    set_text("booking_status", fields.get("booking_status") or "")
    set_text("currency", fields.get("currency") or "")
    set_number("total_amount", fields.get("total_amount") or "")
    set_number("amount_paid", fields.get("amount_paid") or "")
    set_number("amount_due", fields.get("amount_due") or "")
    set_long_text("source_text", fields.get("source_text") or "")
    set_text("language", fields.get("language") or "")
    set_number("adults", fields.get("adults") or "")
    set_number("children", fields.get("children") or "")
    set_number("infants", fields.get("infants") or "")
    set_number("pets", fields.get("pets") or "")
    set_number("people", fields.get("people") or "")
    set_text("key_code", fields.get("key_code") or "")
    set_text("thread_uid", fields.get("thread_uid") or "")
    set_date("created_at", fields.get("created_at") or "")
    set_date("updated_at", fields.get("updated_at") or "")
    set_date("canceled_at", fields.get("canceled_at") or "")

    return out

def upsert_booking(fields: dict) -> dict:
    """
    If item with same Booking ID exists -> update columns, else create new.
    """
    item_name = f"{fields.get('guest') or 'Guest'} — {fields.get('property') or ''} — {fields.get('check_in') or ''}"
    col_vals = build_column_values(fields)
    # find by booking id
    existing_id = find_item_by_booking_id(fields.get("booking_id", ""))
    if existing_id:
        q = """
        mutation($board: ID!, $item: ID!, $vals: JSON!) {
          change_multiple_column_values(board_id: $board, item_id: $item, column_values: $vals) { id }
        }
        """
        resp = monday_graphql(q, {"board": BOARD_ID, "item": int(existing_id), "vals": json.dumps(col_vals)})
        return {"action": "update", "resp": resp}
    else:
        q = """
        mutation($board: ID!, $name: String!, $vals: JSON!) {
          create_item(board_id: $board, item_name: $name, column_values: $vals) { id }
        }
        """
        resp = monday_graphql(q, {"board": BOARD_ID, "name": item_name, "vals": json.dumps(col_vals)})
        return {"action": "create", "resp": resp}

# ========== ROUTES ==========
@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/columns")
def columns():
    cols = get_board_columns(include_settings=True)
    mapping = build_column_map()
    return jsonify({"board_id": BOARD_ID, "columns": cols, "mapping": mapping})

@app.post("/lodgify-webhook")
def lodgify_webhook():
    try:
        payload = request.get_json(force=True, silent=False) or {}
        fields = extract_fields(payload)
        res = upsert_booking(fields)
        return jsonify({"status": "ok", "fields": fields, "monday": res})
    except Exception as e:
        app.logger.exception("webhook error")
        return jsonify({"status": "error", "error": str(e), "trace": traceback.format_exc()}), 500

@app.get("/lodgify-sync-all")
def lodgify_sync_all():
    try:
        limit = int(request.args.get("limit", "20"))
        skip = int(request.args.get("skip", "0"))
        debug = request.args.get("debug")

        # fetch bookings
        items = fetch_lodgify_bookings()
        if skip:
            items = items[skip:]
        if limit:
            items = items[:limit]

        out_items = []
        ok_count = 0
        err_count = 0

        for b in items:
            try:
                fields = extract_fields(b)
                res = upsert_booking(fields)
                ok = True
                if debug:
                    out_items.append({"fields": fields, "monday": res, "ok": ok})
                ok_count += 1
            except Exception as ie:
                err_count += 1
                if debug:
                    out_items.append({"error": str(ie), "ok": False})

        resp = {"status": "done", "skip": skip, "limit": limit, "ok_count": ok_count, "err_count": err_count}
        if debug:
            resp["items"] = out_items
        return jsonify(resp)
    except Exception as e:
        app.logger.exception("sync-all error")
        return jsonify({"status": "error", "error": str(e), "trace": traceback.format_exc()}), 500

# ========== ENTRY ==========
@app.get("/")
def root():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
