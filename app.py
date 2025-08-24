from flask import Flask, request, jsonify
import requests, json, os

app = Flask(__name__)

# Environment variables (Render-ზე დააყენე)
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))

@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/lodgify-webhook", methods=["POST"])
def lodgify_webhook():
    payload = request.json or {}

    # Lodgify JSON-დან ძირითადი ველები
    booking_id = payload.get("id")
    guest = payload.get("guest", {}).get("name", "N/A")
    email = payload.get("guest", {}).get("email", "")
    check_in = payload.get("check_in_date")
    check_out = payload.get("check_out_date")
    property_name = payload.get("property", {}).get("name", "N/A")

    # Column values (შესაბამისი სვეტების ID-ებით, რომლებსაც უკვე შექმენი Monday-ზე)
    colvals = {
        "booking_id": {"text": str(booking_id)},
        "guest": {"text": guest},
        "email": {"email": email, "text": email},
        "check_in": {"date": check_in},
        "check_out": {"date": check_out},
        "property": {"text": property_name}
    }

    # GraphQL მუტაცია
    mutation = """
    mutation ($board: Int!, $item: String!, $colvals: JSON!) {
      create_item(board_id: $board, item_name: $item, column_values: $colvals) {
        id
      }
    }
    """

    variables = {
        "board": BOARD_ID,
        "item": f"Booking {booking_id}",
        "colvals": json.dumps(colvals)
    }

    response = requests.post(
        "https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"},
        json={"query": mutation, "variables": variables}
    )

    return jsonify({
        "status": "ok",
        "booking_id": booking_id,
        "monday_response": response.json()
    })
