from flask import Flask, request, jsonify
import requests, json, os

app = Flask(__name__)

# ---------- ENV ----------
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

LODGY_HEADERS = {"X-ApiKey": LODGY_API_KEY}
MONDAY_HEADERS = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}

# Cache column id map in-memory
COLUMN_ID_MAP = None

# ---------- Lodgify helpers ----------
def fetch_lodgify_bookings():
    """
    Lodgify v2 bookings endpoint returns:
    { "count": null|number, "items": [ { id, arrival, departure, property:{name}, guest:{name,email}, ... }, ... ] }
    """
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
    """
    Normalize Lodgify booking object into fields expected by Monday.
    Be conservative: if some field missing, return empty-safe values.
    """
    bid = b.get("id") or b.get("bookingId")
    guest_name = (b.get("guest") or {}).get("name") or "N/A"
    guest_email = (b.get("guest") or {}).get("email") or ""
    check_in = b.get("arrival") or b.get("check_in_date") or ""
    check_out = b.get("departure") or b.get("check_out_date") or ""
    prop_name = ""
    prop = b.get("property")
    if isinstance(prop, dict):
        prop_name = prop.get("name") or ""
    return {
        "booking_id": str(bid) if bid is not None else "",
        "guest": guest_name,
        "email": guest_email,
        "check_in": check_in,     # YYYY-MM-DD
        "check_out": check_out,   # YYYY-MM-DD
        "property": prop_name
    }

# ---------- Monday helpers ----------
def monday_graphql(query: str, variables: dict):
    r = requests.post("https://api.monday.com/v2", headers=MONDAY_HEADERS, json={"query": query, "variables": variables}, timeout=60)
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
        columns {
          id
          title
          type
        }
      }
    }
    """
    resp = monday_graphql(q, {"board": [BOARD_ID]})
    cols = []
    try:
        cols = resp["data"]["boards"][0]["columns"]
    except Exception:
        app.logger.error(f"Cannot read board columns. Response: {resp}")
    return cols

def ensure_columns_and_get_map():
    """
    Ensure board has required columns; create missing ones.
    Return dict: logical_key -> column_id
    """
    global COLUMN_ID_MAP
    if COLUMN_ID_MAP:
        return COLUMN_ID_MAP

    required = [
        {"key": "booking_id", "title": "Booking ID", "type": "text"},
        {"key": "guest",      "title": "Guest",      "type": "text"},
        {"key": "email",      "title": "Email",      "type": "email"},
        {"key": "check_in",   "title": "Check in",   "type": "date"},
        {"key": "check_out",  "title": "Check out",  "type": "date"},
        {"key": "property",   "title": "Property",   "type": "text"},
    ]

    cols = get_board_columns()

    def find_col_id_by_title_or_id(wanted_title, wanted_id_guess=None):
        # 1) exact id match
        if wanted_id_guess:
            for c in cols:
                if c["id"] == wanted_id_guess:
                    return c["id"]
        # 2) title case-insensitive match
        low = wanted_title.strip().lower()
        for c in cols:
            if (c.get("title") or "").strip().lower() == low:
                return c["id"]
        return None

    # Try to map existing by title or our previous ids (if already exist with same id)
    id_map = {}
    for r in required:
        cid = find_col_id_by_title_or_id(r["title"], r.get("key"))  # try id==key fallback
        if cid:
            id_map[r["key"]] = cid
        else:
            # Create missing column
            create_q = """
            mutation($board: ID!, $title: String!, $ctype: ColumnType!) {
              create_column(board_id: $board, title: $title, column_type: $ctype) {
                id
              }
            }
            """
            resp = monday_graphql(create_q, {"board": BOARD_ID, "title": r["title"], "ctype": r["type"]})
            try:
                new_id = resp["data"]["create_column"]["id"]
                id_map[r["key"]] = new_id
                # refresh local columns cache
                cols.append({"id": new_id, "title": r["title"], "type": r["type"]})
            except Exception:
                app.logger.error(f"Failed to create column '{r['title']}' ({r['type']}), resp={resp}")
                # If create failed, last resort: use the logical key, but mutations will likely fail.
                id_map[r["key"]] = r["key"]

    COLUMN_ID_MAP = id_map
    app.logger.info(f"Column map resolved: {COLUMN_ID_MAP}")
    return COLUMN_ID_MAP

def find_item_by_booking_id(booking_id: str):
    """
    Scan first 500 items and match column 'Booking ID' text to booking_id.
    (Simple & robust without advanced filters.)
    """
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
        app.logger.error(f"find_item_by_booking_id: bad response {resp}")
        return None

    colmap = ensure_columns_and_get_map()
    bid_col_id = colmap["booking_id"]

    for it in items:
        for cv in it.get("column_values", []):
            if cv.get("id") == bid_col_id and (cv.get("text") or "") == booking_id:
                return int(it["id"])
    return None

def build_column_values(fields: dict):
    """
    Convert normalized fields -> Monday column JSON, using discovered column IDs.
    Remove empty values to avoid Monday validation errors.
    """
    colmap = ensure_columns_and_get_map()
    colvals = {}

    def add_text(key, val):
        if val is not None and str(val).strip() != "":
            colvals[colmap[key]] = {"text": str(val)}

    def add_date(key, val):
        v = (val or "").strip()
        if v:
            colvals[colmap[key]] = {"date": v}

    def add_email(key, val):
        v = (val or "").strip()
        if v:
            colvals[colmap[key]] = {"email": v, "text": v}

    add_text("booking_id", fields.get("booking_id"))
    add_text("guest",      fields.get("guest"))
    add_email("email",     fields.get("email"))
    add_date("check_in",   fields.get("check_in"))
    add_date("check_out",  fields.get("check_out"))
    add_text("property",   fields.get("property"))

    return colvals

def upsert_to_monday(fields: dict):
    """
    If item with same Booking ID exists -> update, else create.
    """
    booking_id = fields.get("booking_id") or ""
    if not booking_id:
        return {"errors": [{"message": "missing booking_id"}]}

    item_id = find_item_by_booking_id(booking_id)
    colvals = build_column_values(fields)

    if item_id:
        # Update
        q = """
        mutation($item: ID!, $board: ID!, $vals: JSON!) {
          change_multiple_column_values(item_id: $item, board_id: $board, column_values: $vals) {
            id
          }
        }
        """
        resp = monday_graphql(q, {"item": item_id, "board": BOARD_ID, "vals": json.dumps(colvals)})
        return resp
    else:
        # Create
        q = """
        mutation($board: ID!, $name: String!, $vals: JSON!) {
          create_item(board_id: $board, item_name: $name, column_values: $vals) {
            id
          }
        }
        """
        name = f"Booking {booking_id}"
        resp = monday_graphql(q, {"board": BOARD_ID, "name": name, "vals": json.dumps(colvals)})
        return resp

# ---------- Routes ----------
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/columns", methods=["GET"])
def columns():
    """
    Inspect board columns + show resolved mapping.
    Useful for debugging wrong column IDs.
    """
    cols = get_board_columns()
    cmap = ensure_columns_and_get_map()
    return jsonify({"board_id": BOARD_ID, "columns": cols, "mapping": cmap})

@app.route("/lodgify-webhook", methods=["POST"])
def lodgify_webhook():
    """
    Accept one booking payload from Lodgify webhook and upsert to Monday.
    """
    b = request.json or {}
    fields = extract_booking_fields(b)
    app.logger.info(f"Webhook upsert BookingID={fields.get('booking_id')}")
    resp = upsert_to_monday(fields)
    return jsonify({"status": "ok", "fields": fields, "monday": resp})

@app.route("/lodgify-sync-all", methods=["GET"])
def lodgify_sync_all():
    """
    One-time sync of existing bookings.
    Optional query param: ?limit=5 (default 5 to avoid timeouts on free Render).
    """
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
        app.logger.info(f"Synced booking {fields.get('booking_id')} ({fields.get('check_in')}→{fields.get('check_out')})")

    return jsonify({"status": "done", "processed": len(processed), "limit": limit, "sample": processed})
