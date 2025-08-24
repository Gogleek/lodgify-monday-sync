# app.py
from flask import Flask, request, jsonify
import requests, json, os, traceback
from datetime import datetime, date

app = Flask(__name__)

# ===== ENV =====
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

if not MONDAY_API_TOKEN or not LODGY_API_KEY:
    raise RuntimeError("Set MONDAY_API_TOKEN and LODGY_API_KEY env vars.")

LODGY_HEADERS = {"X-ApiKey": LODGY_API_KEY}
MONDAY_HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

# caches
COLUMN_ID_MAP = None            # monday column mapping
PROPS_CACHE = None              # property.name -> property.id


# ===== Lodgify =====
def fetch_lodgify_bookings():
    url = "https://api.lodgify.com/v2/reservations/bookings"
    r = requests.get(url, headers=LODGY_HEADERS, timeout=30)
    if r.status_code != 200:
        app.logger.error(f"Lodgify error {r.status_code}: {r.text[:300]}")
        return []
    try:
        data = r.json()
    except Exception as e:
        app.logger.exception(f"Lodgify JSON parse error: {e}")
        return []
    return data.get("items", [])


def fetch_lodgify_properties_map():
    """Map property.name -> property.id (fallback თუ booking-ში id არაა)."""
    global PROPS_CACHE
    if PROPS_CACHE is not None:
        return PROPS_CACHE

    url = "https://api.lodgify.com/v1/properties"
    mp = {}
    try:
        r = requests.get(url, headers=LODGY_HEADERS, timeout=30)
        if r.status_code == 200:
            for p in r.json():
                nm = (p.get("name") or "").strip()
                if nm:
                    mp[nm] = p.get("id")
    except Exception:
        pass
    PROPS_CACHE = mp
    return mp


def parse_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def days_between(a: str | None, b: str | None):
    if not a or not b:
        return ""
    da, db = parse_date(a), parse_date(b)
    if not da or not db:
        return ""
    return str((db - da).days)


def derive_source(booking: dict) -> str:
    src = booking.get("source") or booking.get("channel") or ""
    if src:
        return str(src)
    em = (booking.get("guest") or {}).get("email") or ""
    em = em.lower()
    if "guest.booking.com" in em:
        return "Booking.com"
    if "airbnb.com" in em:
        return "Airbnb"
    if "expedia" in em:
        return "Expedia"
    if "vrbo" in em or "homeaway" in em:
        return "Vrbo"
    return ""


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
    bid = b.get("id") or b.get("bookingId")

    guest = b.get("guest") or {}
    prop = b.get("property") or {}

    guest_name = guest.get("name") or "N/A"
    guest_email = guest.get("email") or ""

    check_in = b.get("arrival") or b.get("check_in_date") or ""
    check_out = b.get("departure") or b.get("check_out_date") or ""

    prop_name = prop.get("name") or "" if isinstance(prop, dict) else ""
    prop_id = ""
    if isinstance(prop, dict):
        pid = prop.get("id")
        if pid is None and prop_name:
            pid = fetch_lodgify_properties_map().get(prop_name)
        if pid is not None:
            prop_id = str(pid)

    nights = days_between(check_in, check_out)
    source = derive_source(b)
    status_label = compute_status_label(check_in, check_out)

    return {
        "booking_id": str(bid) if bid is not None else "",
        "guest": guest_name,
        "email": guest_email,
        "check_in": check_in,
        "check_out": check_out,
        "property": prop_name,
        "property_id": prop_id,
        "nights": nights,
        "source": source,
        "status_label": status_label,
        "last_sync": date.today().strftime("%Y-%m-%d"),
        "raw_json": json.dumps(b, ensure_ascii=False)[:15000],  # უსაფრთხო ჭრა
    }


# ===== Monday Core =====
def monday_graphql(query: str, variables: dict):
    r = requests.post(
        "https://api.monday.com/v2",
        headers=MONDAY_HEADERS,
        json={"query": query, "variables": variables},
        timeout=60,
    )
    try:
        return r.json()
    except Exception:
        return {"errors": [{"message": f"Bad JSON from Monday: {r.status_code}", "raw": r.text[:300]}]}


def get_board_columns(include_settings: bool = False):
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
        app.logger.error(f"Cannot read board columns. Response: {resp}")
        return []


def ensure_columns_and_get_map() -> dict:
    """
    Required columns (exact titles):
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
        # create if missing
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
            app.logger.error(f"Failed to create column '{title}' ({ctype}). resp={resp}")
            id_map[key] = title  # worst-case placeholder (mutations შეიძლება ჩავარდეს)

    COLUMN_ID_MAP = id_map
    app.logger.info(f"Column map: {COLUMN_ID_MAP}")
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
        app.logger.error(f"find_item_by_booking_id bad response: {resp}")
        return None

    bid_col = ensure_columns_and_get_map()["booking_id"]
    for it in items:
        for cv in it.get("column_values", []):
            if cv.get("id") == bid_col and (cv.get("text") or "") == booking_id:
                return int(it["id"])
    return None


def parse_settings_labels(settings_str: str | None) -> list[str]:
    """Extract labels list (for status/dropdown) from settings_str JSON."""
    try:
        s = json.loads(settings_str or "{}")
    except Exception:
        return []
    labels_map = s.get("labels") or {}
    if isinstance(labels_map, dict):
        # {"0":"Working on it","1":"Done", ...}
        try:
            return [v for k, v in sorted(labels_map.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 999)]
        except Exception:
            return list(labels_map.values())
    if isinstance(labels_map, list):
        return labels_map
    return []


def build_column_values(fields: dict) -> dict:
    """
    Formats:
      TEXT      -> plain string
      EMAIL     -> {"email":"...","text":"..."}
      DATE      -> {"date":"YYYY-MM-DD"}
      NUMBERS   -> plain string (e.g. "3")
      DROPDOWN  -> {"labels": ["..."]}
      STATUS    -> {"label": "In house"}  (ONLY if label exists on the board)
      LONG_TEXT -> {"text": "..."}
    """
    cmap = ensure_columns_and_get_map()
    colvals: dict[str, object] = {}

    # helpers
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
        v = (label or "").strip()
        if v:
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
            app.logger.info(f"Status label '{label}' not present. Skipping status set.")

    # map fields
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


# ===== Safe wrapper for batch =====
def safe_upsert(booking: dict, debug: bool = False) -> dict:
    """Upsert one booking safely; never raise."""
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


# ===== Routes =====
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
      - ?debug=1   => include per-item error details
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
    ok_count = 0
    err_count = 0

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
