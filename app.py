import os, json, logging
from logging.handlers import RotatingFileHandler
from flask import Flask, request, jsonify
from services.lodgify import LodgifyClient
from services.monday import MondayClient, UpsertResult
from utils.phone import normalize_phone
from mapping import COLUMN_MAP, DROPDOWN_LABELS
from datetime import datetime

app = Flask(__name__)

# Logging setup
level = os.getenv("LOG_LEVEL", "INFO").upper()
handler = RotatingFileHandler("app.log", maxBytes=1_000_000, backupCount=3)
logging.basicConfig(level=level, handlers=[handler, logging.StreamHandler()])
log = logging.getLogger("lodgify-monday")

# Clients
lodgify = LodgifyClient(
    api_base=os.getenv("LODGY_API_BASE", "https://api.lodgify.com"),
    api_key=os.getenv("LODGY_API_KEY", ""),
)

monday = MondayClient(
    api_base=os.getenv("MONDAY_API_BASE", "https://api.monday.com/v2"),
    api_key=os.getenv("MONDAY_API_KEY", ""),
    board_id=int(os.getenv("MONDAY_BOARD_ID", "0")),
)

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "lodgify-monday",
        "board_id": monday.board_id
    }), 200

@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "endpoints": ["/health", "/lodgify-sync-all", "/webhook/lodgify"]
    }), 200

@app.post("/webhook/lodgify")
def webhook_lodgify():
    payload = request.get_json(silent=True) or {}
    log.info("Webhook/Lodgify: %s", json.dumps(payload)[:2000])

    booking = payload.get("booking") or payload
    if not booking:
        return jsonify({"ok": False, "error": "No booking payload"}), 400

    try:
        mapped = map_booking_to_monday(booking)
        res: UpsertResult = monday.upsert_item(mapped)
        return jsonify({"ok": True, "result": res.to_dict(), "source": "webhook"}), 200
    except Exception as e:
        log.exception("Webhook processing failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/lodgify-sync-all")
def lodgify_sync_all():
    """Batch sync from Lodgify → Monday"""
    limit = int(request.args.get("limit", 50))
    skip = int(request.args.get("skip", 0))
    debug = request.args.get("debug", "0") == "1"

    try:
        bookings = lodgify.list_bookings(limit=limit, skip=skip)
        log.info("Fetched %d booking(s) [limit=%s, skip=%s]", len(bookings), limit, skip)

        results = []
        for bk in bookings:
            try:
                mapped = map_booking_to_monday(bk)
                res: UpsertResult = monday.upsert_item(mapped)
                results.append(res.to_dict())
            except Exception as e:
                log.exception("Failed to upsert booking id=%s", bk.get("id"))
                results.append({
                    "ok": False,
                    "error": str(e),
                    "source_id": bk.get("id")
                })

        response = {"ok": True, "count": len(results), "results": results}
        if debug and bookings:
            response["sample_input"] = bookings[:1]
            response["sample_mapped"] = map_booking_to_monday(bookings[0])
        return jsonify(response), 200

    except Exception as e:
        log.exception("Batch sync failed")
        return jsonify({"ok": False, "error": str(e)}), 500

# ——————————————————————————
# Mapping logic
# ——————————————————————————
def map_booking_to_monday(bk: dict) -> dict:
    guest = bk.get("guest") or {}
    phone = normalize_phone(guest.get("phone") or guest.get("mobile") or "")
    email = guest.get("email") or ""
    first_name = guest.get("first_name") or guest.get("name") or ""
    last_name = guest.get("last_name") or ""

    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "")
    unit_name = (bk.get("rental") or {}).get("name") or bk.get("unit_name") or "Unknown unit"
    status = (bk.get("status") or "").lower()

    status_label = DROPDOWN_LABELS.get("status", {}).get(
        status,
        DROPDOWN_LABELS["status"].get("default", "Pending")
    )

    check_in = bk.get("check_in") or bk.get("start_date")
    check_out = bk.get("check_out") or bk.get("end_date")
    total_price = bk.get("total") or bk.get("price_total") or 0
    currency = bk.get("currency") or "GBP"

    item_name = f"{first_name} {last_name}".strip() or f"Booking {res_id}"

    cv = {}
    put(cv, "reservation_id", res_id)
    put(cv, "unit", unit_name)
    put(cv, "email", email)
    put(cv, "phone", phone)
    put(cv, "check_in", {"date": to_date(check_in)})
    put(cv, "check_out", {"date": to_date(check_out)})
    put(cv, "total", total_price)
    put(cv, "currency", currency)
    put(cv, "status", {"labels": [status_label]})

    if email:
        cv[COLUMN_MAP["assignee"]] = {
            "personsAndTeams": [{"id": email, "kind": "person"}]
        }

    return {
        "item_name": item_name,
        "external_id": res_id,
        "column_values": cv,
    }

def to_date(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).date().isoformat()
    except Exception:
        try:
            return datetime.strptime(str(v)[:10], "%Y-%m-%d").date().isoformat()
        except Exception:
            return None

def put(cv: dict, logical_key: str, value):
    col_id = COLUMN_MAP.get(logical_key)
    if col_id:
        cv[col_id] = value

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
