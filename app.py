from flask import Flask, request, jsonify
import requests, json, os

app = Flask(__name__)

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")  # Lodgify API key


def find_existing_item(booking_id):
    query = """
    query ($board: Int!) {
      boards(ids: [$board]) {
        items_page {
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
    data = resp.json()
    try:
        items = data["data"]["boards"][0]["items_page"]["items"]
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
    url = "https://api.lodgify.com/v2/bookings"   # აქ თუ შეცდომაა, გამოჩნდება
    headers = {"X-ApiKey": LODGY_API_KEY}
    resp = requests.get(url, headers=headers)

    # Debug output
    return jsonify({
        "lodgify_status": resp.status_code,
        "lodgify_url": url,
        "lodgify_text": resp.text[:1000]  # პირველი 1000 სიმბოლო, რომ დავინახოთ error ან data
    })
