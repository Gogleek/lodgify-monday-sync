from flask import Flask, request, jsonify
import requests, os, json

app = Flask(__name__)

# ENV VARIABLES
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

LODGY_BASE = "https://api.lodgify.com/v1"

# === Lodgify Helpers ===
def fetch_properties():
    url = f"{LODGY_BASE}/properties"
    resp = requests.get(url, headers={"X-ApiKey": LODGY_API_KEY})
    return resp.json() if resp.status_code == 200 else []

def fetch_reservations():
    """Get all reservations (bookings) with pagination"""
    all_res = []
    page = 1
    while True:
        url = f"{LODGY_BASE}/reservations?page={page}&pageSize=50"
        resp = requests.get(url, headers={"X-ApiKey": LODGY_API_KEY})
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        all_res.extend(data)
        if len(data) < 50:
            break
        page += 1
    return all_res

# === Monday Helpers ===
def find_existing_item(booking_id):
    query = """
    query ($board: Int!) {
      boards(ids: [$board]) {
        items_page(limit: 200) {
          items {
            id
            column_values {
              id
              text
            }
          }
        }
      }
    }
    """
    resp = requests.post(
        "https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"},
        json={"query": query, "variables": {"board": BOARD_ID}}
    )
    try:
        items = resp.json()["data"]["boards"][0]["items_page"]["items"]
        for it in items:
            for col in it["column_values"]:
                if col["id"] == "booking_id" and col["text"] == str(booking_id):
                    return it["id"]
    except Exception:
        pass
    return None

def upsert_booking(booking):
    booking_id = booking.get("id")
    guest = booking.get("guest", {}).get("name", "N/A")
    email = booking.get("guest", {}).get("email", "")
    check_in = booking.get("check_in_date")
    check_out = booking.get("check_out_date")
    property_name = booking.get("property", {}).get("name", "N/A")

    colvals = {
        "booking_id": {"text": str(booking_id)},
        "guest": {"text": guest},
        "email": {"email": email, "text": email},
        "check_in": {"date": check_in},
        "check_out": {"date": check_out},
        "property": {"text": property_name}
    }

    existing = find_existing_item(booking_id)

    if existing:
        mutation = """
        mutation ($item: Int!, $colvals: JSON!) {
          change_multiple_column_values(item_id: $item, board_id: %d, column_values: $colvals) {
            id
          }
        }
        """ % BOARD_ID
        variables = {"item": int(existing), "colvals": json.dumps(colvals)}
    else:
        mutation = """
        mutation ($board: Int!, $item: String!, $colvals: JSON!) {
          create_item(board_id: $board, item_name: $item, column_values: $colvals) {
            id
          }
        }
        """
        variables = {"board": BOARD_ID, "item": f"Booking {booking_id}", "colvals": json.dumps(colvals)}

    r = requests.post(
        "https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"},
        json={"query": mutation, "variables": variables}
    )
    return r.json()

# === Flask Routes ===
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/lodgify-webhook", methods=["POST"])
def lodgify_webhook():
    payload = request.json or {}
    result = upsert_booking(payload)
    return jsonify({"status": "ok", "monday_response": result})

@app.route("/lodgify-sync-all", methods=["GET"])
def lodgify_sync_all():
    reservations = fetch_reservations()
    results = []
    for r in reservations:
        results.append(upsert_booking(r))

    return jsonify({
        "status": "done",
        "count": len(results),
        "sample": results[:3]   # მხოლოდ პირველი 3 რომ რენდერი არ გაწყდეს
    })

@app.route("/lodgify-properties", methods=["GET"])
def lodgify_properties():
    props = fetch_properties()
    return jsonify({"count": len(props), "properties": props[:5]})
