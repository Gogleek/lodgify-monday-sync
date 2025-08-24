from flask import Flask, request, jsonify
import requests, json, os

app = Flask(__name__)

# ---------- ENV ----------
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

LODGY_HEADERS = {"X-ApiKey": LODGY_API_KEY}
MONDAY_HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

COLUMN_ID_MAP = None  # cache

# ---------- Lodgify ----------
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

def extract_booking_fields(b):
    bid = b.get("id") or b.get("bookingId")
    guest_name = (b.get("guest") or {}).get("name") or "N/A"
    guest_email = (b.get("guest") or {}).get("email") or ""
    check_in = b.get("arrival") or b.get("check_in_date") or ""
    check_out = b.get("departure") or b.get("check_out_date") or ""
    prop_name = ""
    if isinstance(b.get("property"), dict):
        prop_name = b["property"].get("name") or ""
    return {
        "booking_id": str(bid) if bid is not None else "",
        "guest": guest_name,
        "email": guest_email,
        "check_in": check_in,
        "check_out": check_out,
        "property": prop_name,
    }

# ---------- Monday ----------
def monday_graphql(query: str, variables: dict):
    r = requests.post("https://api.monday.com/v2",
                      headers=MONDAY_HEADERS,
                      json={"query": query, "variables": variables},
                      timeout=60)
    try:
        return r.json()
    except Exception:
        return {"errors": [{"message": f"Bad JSON from Monday: {r.status_code}", "raw": r.text[:300]}]}

def get_board_columns():
    q = """
    query($board: [ID!]!) {
      boards(ids: $board) {
        id
        name
        columns { id title type }
      }
    }
    """
    resp = monday_graphql(q, {"board": [BOARD_ID]})
    try:
        return resp["data"]["boards"][0]["columns"]
    except Exception:
        app.logger.error(f"Cannot read board columns. Response: {resp}")
        return []

def ensure_columns_and_get_map():
    global COLUMN_ID_MAP
    if COLUMN_ID_MAP:
        return COLUMN_ID_MAP

    # ColumnType enum MUST be uppercase
    required = [
        {"key": "booking_id", "title": "Booking ID", "type": "TEXT"},
        {"key": "guest",      "title": "Guest",      "type": "TEXT"},
        {"key": "email",      "title": "Email",      "type": "EMAIL"},
        {"key": "check_in",   "title": "Check in",   "type": "DATE"},
        {"key": "check_out",  "title": "Check out",  "type": "DATE"},
        {"key": "property",   "title": "Property",   "type": "TEXT"},
    ]

    cols = get_board_columns()

    def find_col_id_by_title(wanted_title):
        low = wanted_title.strip().lower()
        for c in cols:
            if (c.get("title") or "").strip().lower() == low:
                return c["id"]
        return None

    id_map = {}
    for r in required:
        cid = find_col_id_by_title(r["title"])
        if cid:
            id_map[r["key"]] = cid
            continue

        create_q = """
        mutation($board: ID!, $title: String!, $ctype: ColumnType!) {
          create_column(board_id: $board, title: $title, column_type: $ctype) { id }
        }
        """
        resp = monday_graphql(create_q, {"board": BOARD_ID, "title": r["title"], "ctype": r["type"]})
        try:
            new_id = resp["data"]["create_column"]["id"]
            id_map[r["key"]] = new_id
            cols.append({"id": new_id, "title": r["title"], "type": r["type"]})
        except Exception:
            app.logger.error(f"Failed to create column '{r['title']}' ({r['type']}), resp={resp}")
            id_map[r["key"]] = r["title"]  # fallback (mutation შესაძლოა ჩავარდეს)

    COLUMN_ID_MAP = id_map
    app.logger.info(f"Column map: {COLUMN_ID_MAP}")
    return COLUMN_ID_MAP

def find_item_by_booking_id(booking_id: str):
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

def build_column_values(fields: dict):
    """
    - TEXT  => plain string
    - EMAIL => {"email": "...", "text": "..."}
    - DATE  => {"date": "YYYY-MM-DD"}
    """
    cmap = ensure_columns_and_get_map()
    colvals = {}

    def set_text(key, val):
        v = (val or "").strip()
        if v:
            colvals[cmap[key]] = v  # IMPORTANT: plain string

    def set_date(key, val):
        v = (val or "").strip()
        if v:
            colvals[cmap[key]] = {"date": v}

    def set_email(key, val):
        v = (val or "").strip()
        if v:
            colvals[cmap[key]] = {"email": v, "text": v}

    set_text("booking_id", fields.get("booking_id"))
    set_text("guest",      fields.get("guest"))
    set_email("email",     fields.get("email"))
    set_date("check_in",   fields.get("check_in"))
    set_date("check_out",  fields.get("check_out"))
    set_text("property",   fields.get("property"))

    return colvals

def sanitize_text_objects(colvals: dict):
    """
    Safety net: თუ შემთხვევით შემორჩა {"text": "..."} — გადააკეთე "..."-ად.
    """
    fixed = {}
    for k, v in colvals.items():
        if isinstance(v, dict) and set(v.keys()) == {"text"}:
            fixed[k] = v["text"]
        else:
            fixed[k] = v
    return fixed

def upsert_to_monday(fields: dict):
    booking_id = fields.get("booking_id") or ""
    if not booking_id:
        return {"errors": [{"message": "missing booking_id"}]}

    item_id = find_item_by_booking_id(booking_id)
    colvals = sanitize_text_objects(build_column_values(fields))
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

# ---------- Routes ----------
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/columns")
def columns():
    cols = get_board_columns()
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
    # safe default limit on free Render
    try:
        limit = int(request.args.get("limit", "5"))
        if limit <= 0:
            limit = 5
    except Exception:
        limit = 5

    items = fetch_lodgify_bookings()
    processed = []
    for i, b in enumerate(items):
        if i >= limit:
            break
        fields = extract_booking_fields(b)
        resp = upsert_to_monday(fields)
        processed.append({"fields": fields, "monday": resp})
        app.logger.info(
            f"Synced booking {fields.get('booking_id')} ({fields.get('check_in')}→{fields.get('check_out')})"
        )

    return jsonify({"status": "done", "processed": len(processed), "limit": limit, "sample": processed})
