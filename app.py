# app.py
from flask import Flask, request, jsonify
import os, json, re, traceback
from datetime import datetime, date
import requests

app = Flask(__name__)

# ======================
# ENV & CONSTANTS
# ======================
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

if not MONDAY_API_TOKEN or not LODGY_API_KEY:
    raise RuntimeError("Set MONDAY_API_TOKEN and LODGY_API_KEY env vars.")

MONDAY_HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
LODGY_HEADERS = {"X-ApiKey": LODGY_API_KEY}

# Caches
COLUMN_ID_MAP: dict | None = None
PROPS_BY_ID: dict | None = None
PROPS_BY_NAME: dict | None = None

# dropdown known labels (extendable)
DEFAULT_SOURCE_LABELS = ["Booking.com", "Airbnb", "Expedia", "Vrbo", "Manual", "Direct"]

# Source normalization map (lowercased, non-alnum stripped)
SOURCE_NORMALIZE_MAP = {
    "bookingcom": "Booking.com",
    "booking.com": "Booking.com",
    "bookingcomheavenlystays": "Booking.com",
    "bookingcomheavenlystaysqueensgardens": "Booking.com",
    "airbnb": "Airbnb",
    "expedia": "Expedia",
    "vrbo": "Vrbo",
    "homeaway": "Vrbo",
    "manual": "Manual",
    "direct": "Direct",
}

# ======================
# Lodgify helpers
# ======================
def lodgify_get(url: str, **kw):
    r = requests.get(url, headers=LODGY_HEADERS, timeout=kw.pop("timeout", 30))
    return r

def fetch_lodgify_bookings() -> list[dict]:
    """
    v2/reservations/bookings → {"count": X, "items": [ ... ]}
    """
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

def fetch_lodgify_properties_index():
    """
    v1/properties → list of {id, name, ...}
    Builds both id→name and name→id maps.
    """
    global PROPS_BY_ID, PROPS_BY_NAME
    if PROPS_BY_ID is not None and PROPS_BY_NAME is not None:
        return PROPS_BY_ID, PROPS_BY_NAME

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

    PROPS_BY_ID, PROPS_BY_NAME = id_map, name_map
    return PROPS_BY_ID, PROPS_BY_NAME

def parse_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def days_between(a: str | None, b: str | None) -> str:
    if not a or not b:
        return ""
    da, db = parse_date(a), parse_date(b)
    if not da or not db:
        return ""
    return str((db - da).days)

def normalize_source_label(raw: str | None) -> str:
    if not raw:
        return ""
    key = re.sub(r"[^a-z0-9]+", "", str(raw).lower())
    return SOURCE_NORMALIZE_MAP.get(key, raw.strip())

def derive_source(b: dict) -> str:
    # prefer Lodgify native source/channel
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

def extract_booking_fields(b: dict) -> dict:
    """
    Flattens Lodgify booking into Monday fields.
    """
    # IDs
    booking_id = b.get("id") or b.get("bookingId")
    booking_id = str(booking_id) if booking_id is not None else ""

    # Dates
    check_in = b.get("arrival") or b.get("check_in_date") or ""
    check_out = b.get("departure") or b.get("check_out_date") or ""
    nights = days_between(check_in, check_out)

    # Guest
    g = b.get("guest") or {}
    guest_name = g.get("name") or "N/A"
    guest_email = g.get("email") or ""
    guest_phone = g.get("phone") or ""

    # Property
    prop_id = b.get("property_id")
    prop_name = ""
    # Some payloads include {property: {id, name}}, support that too:
    prop = b.get("property") if isinstance(b.get("property"), dict) else None
    if prop is not None:
        prop_id = prop_id or prop.get("id")
        prop_name = prop.get("name") or ""

    if prop_id is not None and str(prop_id):
        prop_id = str(prop_id)
        if not prop_name:
            id_map, _ = fetch_lodgify_properties_index()
            prop_name = id_map.get(prop_id, "")
    else:
        prop_id = ""

    # Source & status
    source = derive_source(b)
    status_label = compute_status_label(check_in, check_out)

    # Raw JSON (truncate for Monday long_text)
    raw_json = json.dumps(b, ensure_ascii=False)
    if len(raw_json) > 15000:
        raw_json = raw_json[:15000]

    return {
        "booking_id": booking_id,
        "guest": guest_name,
        "email": guest_email,
        "phone": guest_phone,          # optional column (if you add it)
        "check_in": check_in,
        "check_out": check_out,
        "nights": nights,
        "property": prop_name,
        "property_id": prop_id,
        "source": source,
        "status_label": status_label,
        "last_sync": date.today().strftime("%Y-%m-%d"),
        "raw_json": raw_json,
        # extra (if you add these columns in Monday, we can map later)
        "booking_status": b.get("status") or "",                 # text
        "currency": b.get("currency_code") or "",                # text
        "total_amount": str((b.get("total_amount") or "")).strip(),  # numbers
        "amount_paid": str((b.get("amount_paid") or "")).strip(),    # numbers
        "amount_due": str((b.get("amount_due") or "")).strip(),      # numbers
        "source_text": b.get("source_text") or "",               # long_text
        "language": b.get("language") or "",                     # text
    }

# ======================
# Monday GraphQL helpers
# ======================
def monday_graphql(query: str, variables: dict):
    r = requests.post("https://api.monday.com/v2",
                      headers=MONDAY_HEADERS,
                      json={"query": query, "variables": variables},
                      timeout=60)
    try:
        return r.json()
    except Exception:
        return {"errors": [{"message": f"Bad JSON {r.status_code}", "raw": r.text[:300]}]}

def get_board_columns(include_settings: bool = False) -> list[dict]:
    extra = " settings_str" if include_settings else ""
    q = f"""
    query($board: [ID!]!) {{
      boards(ids: $board) {{
        id
        name
        columns {{ id title type{extra} }}
      }}
    }}
    """
    resp = monday_graphql(q, {"board": [BOARD_ID]})
    try:
        return resp["data"]["boards"][0]["columns"]
    except Exception:
        app.logger.error(f"[Monday] get_board_columns bad resp: {resp}")
        return []

def ensure_columns_and_get_map() -> dict:
    """
    Ensures required columns exist and returns a map key→column_id.
    Required (exact titles):
      Booking ID(text), Property(text), Property ID(text), Guest(text), Email(email),
      Check-in(date), Check-out(date), Nights(numbers), Source(dropdown),
      Status(status), Last Sync(date), Raw JSON(long_text)
    """
    global COLUMN_ID_MAP
    if COLUMN_ID_MAP:
        return COLUMN_ID_MAP

    required = [
        ("booking_id", "Booking ID", "TEXT"),
        ("property", "Property", "TEXT"),
        ("property_id", "Property ID", "TEXT"),
        ("guest", "Guest", "TEXT"),
        ("email", "Email", "EMAIL"),
        ("check_in", "Check-in", "DATE"),
        ("check_out", "Check-out", "DATE"),
        ("nights", "Nights", "NUMBERS"),
        ("source", "Source", "DROPDOWN"),
        ("status", "Status", "STATUS"),
        ("last_sync", "Last Sync", "DATE"),
        ("raw_json", "Raw JSON", "LONG_TEXT"),
        # Optional extras (uncomment if you already created these on the board):
        # ("phone", "Phone", "PHONE"),
        # ("booking_status", "Booking Status", "TEXT"),
        # ("currency", "Currency", "TEXT"),
        # ("total_amount", "Total Amount", "NUMBERS"),
        # ("amount_paid", "Amount Paid", "NUMBERS"),
        # ("amount_due", "Amount Due", "NUMBERS"),
        # ("source_text", "Source Text", "LONG_TEXT"),
        # ("language", "Language", "TEXT"),
    ]

    cols = get_board_columns(include_settings=True)

    def find_by_title(t: str):
        low = t.strip().lower()
        for c in cols:
            if (c.get("title") or "").strip().lower() == low:
                return c
        return None

    id_map: dict[str, str] = {}
    for key, title, ctype in required:
        c = find_by_title(title)
        if c:
            id_map[key] = c["id"]
            continue
        # create missing column
        q = """
        mutation($board: ID!, $title: String!, $ctype: ColumnType!) {
          create_column(board_id: $board, title: $title, column_type: $ctype) { id }
        }
        """
        resp = monday_graphql(q, {"board": BOARD_ID, "title": title, "ctype": ctype})
        try:
            new_id = resp["data"]["create_column"]["id"]
            id_map[key] = new_id
            cols.append({"id": new_id, "title": title, "type": ctype})
        except Exception:
            app.logger.error(f"[Monday] failed to create column '{title}' ({ctype}). resp={resp}")
            id_map[key] = title  # placeholder

    COLUMN_ID_MAP = id_map
    app.logger.info(f"[Monday] Column map: {COLUMN_ID_MAP}")
    return COLUMN_ID_MAP

def find_item_by_booking_id(booking_id: str) -> int | None:
    q = """
    query($board: [ID!]!) {
      boards(ids: $board) {
        items_page(limit: 500) {
          items {
            id
            column_values { id text }
          }
        }
      }
    }
    """
    resp = monday_graphql(q, {"board": [BOARD_ID]})
    try:
        items = resp["data"]["boards"][0]["items_page"]["items"]
    except Exception:
        app.logger.error(f"[Monday] find_item... bad resp: {resp}")
        return None

    bid_col = ensure_columns_and_get_map()["booking_id"]
    for it in items:
        for cv in it.get("column_values", []):
            if cv.get("id") == bid_col and (cv.get("text") or "") == booking_id:
                return int(it["id"])
    return None

def parse_settings_labels(settings_str: str | None) -> list[str]:
    try:
        s = json.loads(settings_str or "{}")
    except Exception:
        return []
    labels_map = s.get("labels") or {}
    if isinstance(labels_map, dict):
        # {"0":"Working on it","1":"Done",...}
        try:
            # keep index order if numeric keys
            return [v for k, v in sorted(labels_map.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 999)]
        except Exception:
            return list(labels_map.values())
    if isinstance(labels_map, list):
        return labels_map
    return []

def ensure_dropdown_label_exists(column_key: str, wanted: str):
    """For dropdown columns: if label missing, extend labels via change_column_settings."""
    if not wanted:
        return
    cmap = ensure_columns_and_get_map()
    col_id = cmap[column_key]

    cols = get_board_columns(include_settings=True)
    col = next((c for c in cols if c["id"] == col_id), None)
    existing = parse_settings_labels((col or {}).get("settings_str"))

    merged: list[str] = []
    for x in (existing or []) + DEFAULT_SOURCE_LABELS + [wanted]:
        if x and x not in merged:
            merged.append(x)

    if set(existing or []) == set(merged):
        return

    settings_str = json.dumps({"labels": merged})
    q = """
    mutation($board: ID!, $column_id: ID!, $settings_str: String!) {
      change_column_settings(board_id: $board, column_id: $column_id, settings_str: $settings_str) { id }
    }
    """
    resp = monday_graphql(q, {"board": BOARD_ID, "column_id": col_id, "settings_str": settings_str})
    app.logger.info(f"[Monday] Updated dropdown '{col_id}' labels → {merged}. resp={resp}")

def build_column_values(fields: dict) -> dict:
    """
    Mapping to Monday payloads:
      TEXT      -> "string"
      EMAIL     -> {"email":"...","text":"..."}
      DATE      -> {"date":"YYYY-MM-DD"}
      NUMBERS   -> "numeric-string"
      DROPDOWN  -> {"labels":["..."]}  (ensure label exists)
      STATUS    -> {"label":"..."}     (only if label exists; else skip)
      LONG_TEXT -> {"text":"..."}
    """
    cmap = ensure_columns_and_get_map()
    colvals: dict[str, object] = {}

    # --- simple mappers ---
    def set_text(key, val):
        v = (val or "").strip()
        if v:
            colvals[cmap[key]] = v

    def set_number(key, val):
        v = (val or "").strip()
        if v != "":
            colvals[cmap[key]] = v

    def set_date(key, val):
        v = (val or "").strip()
        if v:
            colvals[cmap[key]] = {"date": v}

    def set_email(key, val):
        v = (val or "").strip()
        if v:
            colvals[cmap[key]] = {"email": v, "text": v}

    def set_dropdown(key, label):
        v = normalize_source_label(label or "")
        if v:
            ensure_dropdown_label_exists(key, v)
            colvals[cmap[key]] = {"labels": [v]}

    def set_status_if_exists(label):
        if not label:
            return
        cols = get_board_columns(include_settings=True)
        status_id = cmap["status"]
        st = next((c for c in cols if c["id"] == status_id), None)
        if not st:
            return
        labels = parse_settings_labels(st.get("settings_str"))
        if label in labels:
            colvals[status_id] = {"label": label}
        else:
            app.logger.info(f"[Monday] status label '{label}' absent. Skipping status set.")

    # --- required fields ---
    set_text("booking_id",  fields.get("booking_id"))
    set_text("guest",       fields.get("guest"))
    set_email("email",      fields.get("email"))
    set_text("property",    fields.get("property"))
    set_text("property_id", fields.get("property_id"))
    set_date("check_in",    fields.get("check_in"))
    set_date("check_out",   fields.get("check_out"))
    set_number("nights",    fields.get("nights"))
    set_dropdown("source",  fields.get("source"))
    set_date("last_sync",   fields.get("last_sync"))

    rj = (fields.get("raw_json") or "").strip()
    if rj:
        colvals[cmap["raw_json"]] = {"text": rj}

    set_status_if_exists(fields.get("status_label"))

    # --- optional extras (only if you uncommented them in ensure_columns_and_get_map) ---
    # if "phone" in cmap:           set_text("phone", fields.get("phone"))
    # if "booking_status" in cmap:  set_text("booking_status", fields.get("booking_status"))
    # if "currency" in cmap:        set_text("currency", fields.get("currency"))
    # if "total_amount" in cmap:    set_number("total_amount", fields.get("total_amount"))
    # if "amount_paid" in cmap:     set_number("amount_paid", fields.get("amount_paid"))
    # if "amount_due" in cmap:      set_number("amount_due", fields.get("amount_due"))
    # if "source_text" in cmap:
    #     st = (fields.get("source_text") or "").strip()
    #     if st:
    #         colvals[cmap["source_text"]] = {"text": st}
    # if "language" in cmap:        set_text("language", fields.get("language"))

    return colvals

def upsert_to_monday(fields: dict) -> dict:
    booking_id = (fields.get("booking_id") or "").strip()
    if not booking_id:
        return {"errors": [{"message": "missing booking_id"}]}

    item_id = find_item_by_booking_id(booking_id)
    colvals = build_column_values(fields)
    colvals_json = json.dumps(colvals)

    if item_id:
        q = """
        mutation($item: ID!, $board: ID!, $vals: JSON!) {
          change_multiple_column_values(item_id: $item, board_id: $board, column_values: $vals) { id }
        }
        """
        return monday_graphql(q, {"item": item_id, "board": BOARD_ID, "vals": colvals_json})

    q = """
    mutation($board: ID!, $name: String!, $vals: JSON!) {
      create_item(board_id: $board, item_name: $name, column_values: $vals) { id }
    }
    """
    name = f"Booking {booking_id}"
    return monday_graphql(q, {"board": BOARD_ID, "name": name, "vals": colvals_json})

# ======================
# Batch-safe wrapper
# ======================
def safe_upsert(booking: dict, debug: bool = False) -> dict:
    try:
        fields = extract_booking_fields(booking)
        resp = upsert_to_monday(fields)
        return {"ok": True, "fields": fields, "monday": resp}
    except Exception as e:
        err = {"ok": False, "error": str(e)}
        if debug:
            err["trace"] = traceback.format_exc(limit=3)
            try:
                err["booking_id"] = booking.get("id")
                err["arrival"] = booking.get("arrival")
                err["departure"] = booking.get("departure")
            except Exception:
                pass
        return err

# ======================
# Routes
# ======================
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/columns")
def columns():
    cols = get_board_columns(include_settings=True)
    cmap = ensure_columns_and_get_map()
    return jsonify({"board_id": BOARD_ID, "columns": cols, "mapping": cmap})

@app.route("/lodgify-webhook", methods=["POST"])
def lodgify_webhook():
    b = request.json or {}
    fields = extract_booking_fields(b)
    app.logger.info(f"Webhook upsert BookingID={fields.get('booking_id')}")
    resp = upsert_to_monday(fields)
    return jsonify({"status": "ok", "fields": fields, "monday": resp})

@app.route("/lodgify-sync-all", methods=["GET"])
def lodgify_sync_all():
    """
    Batch sync with resilience:
      - ?limit=20  (default 5)
      - ?skip=0
      - ?debug=1   include per-item errors/trace
    """
    # limit
    try:
        limit = int(request.args.get("limit", "5"))
        if limit <= 0:
            limit = 5
    except Exception:
        limit = 5
    # skip
    try:
        skip = int(request.args.get("skip", "0"))
        if skip < 0:
            skip = 0
    except Exception:
        skip = 0

    debug = request.args.get("debug", "0").lower() in ("1", "true", "yes")

    all_items = fetch_lodgify_bookings()
    window = all_items[skip: skip + limit]

    results = []
    ok_count = err_count = 0

    for b in window:
        r = safe_upsert(b, debug=debug)
        results.append(r)
        if r.get("ok"):
            ok_count += 1
            f = r.get("fields", {})
            app.logger.info(f"Synced booking {f.get('booking_id')} ({f.get('check_in')}→{f.get('check_out')})")
        else:
            err_count += 1
            app.logger.warning(f"Sync error: {r.get('error')}")

    return jsonify({
        "status": "done",
        "skip": skip,
        "limit": limit,
        "ok_count": ok_count,
        "err_count": err_count,
        "items": results,
    })
