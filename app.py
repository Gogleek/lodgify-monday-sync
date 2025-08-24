from flask import Flask, request, jsonify
import requests, json, os

app = Flask(__name__)

# --- ENVIRONMENT VARIABLES ---
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

HEADERS = {
    "X-ApiKey": LODGY_API_KEY,
    "Content-Type": "application/json",
}

# --- Lodgify Helpers ---
def fetch_properties():
    r = requests.get("https://api.lodgify.com/v1/properties", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_bookings():
    """Lodgify → bookings.items ამოიღოს"""
    url = "https://api.lodgify.com/v2/reservations/bookings"
    resp = requests.get(url, headers={"X-ApiKey": LODGY_API_KEY})
    if resp.status_code != 200:
        return []
    data = resp.json()
    return data.get("items", [])

# --- Monday Helpers ---
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
        headers={"Authorization": MONDAY_API_TOKEN, "Content-Type":"application/json"},
        json={"query": query, "variables": {"board": BOARD_ID}}
    )
    try:
        items = resp.json()["data"]["boards"][0]["items_page"]["items"]
        for it in items:
            for col in it["column_values"]:
                if col["id"]=="booking_id" and col["text"]==str(booking_id):
                    return it["id"]
    except Exception:
        pass
    return None

def upsert_booking(b):
    bid = b.get("id") or b.get("bookingId")
    guest = b.get("guest", {}).get("name", "N/A")
    email = b.get("guest", {}).get("email", "")
    check_in = b.get("arrival")
    check_out= b.get("departure")
    pname = b.get("property",{}).get("name") if isinstance(b.get("property"),dict) else "N/A"

    colvals = {
        "booking_id": {"text": str(bid)},
        "guest": {"text": guest},
        "email": {"email": email, "text": email},
        "check_in": {"date": check_in},
        "check_out": {"date": check_out},
        "property": {"text": pname}
    }

    existing = find_existing_item(bid)
    if existing:
        mutation = """
        mutation ($item: Int!, $colvals: JSON!) {
          change_multiple_column_values(item_id:$item, board_id:%d, column_values:$colvals){id}
        }""" % BOARD_ID
        variables={"item": int(existing),"colvals": json.dumps(colvals)}
    else:
        mutation = """
        mutation ($board: Int!, $item: String!, $colvals: JSON!) {
          create_item(board_id:$board, item_name:$item, column_values:$colvals){id}
        }"""
        variables={"board": BOARD_ID,"item": f"Booking {bid}","colvals": json.dumps(colvals)}

    r=requests.post("https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_TOKEN,"Content-Type":"application/json"},
        json={"query": mutation,"variables": variables})
    return r.json()

# --- Flask Routes ---
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/lodgify-properties", methods=["GET"])
def lodgify_props():
    props = fetch_properties()
    return jsonify({"count":len(props),"sample":props[:3]})

@app.route("/lodgify-webhook", methods=["POST"])
def lodgify_webhook():
    payload = request.json or {}
    result = upsert_booking(payload)
    return jsonify({"status":"ok","monday_response":result})

@app.route("/lodgify-sync-all", methods=["GET"])
def lodgify_sync_all():
    bookings = fetch_bookings()
    results = []
    for b in bookings:
        results.append(upsert_booking(b))

    return jsonify({
        "status": "done",
        "count": len(results),
        "sample": results[:3]
    })
