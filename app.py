from flask import Flask, request, jsonify
import requests, json, os, re
from datetime import datetime as dt

app = Flask(__name__)

# --- ENV VARIABLES ---
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")
BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "2112686712"))
LODGY_API_KEY = os.getenv("LODGY_API_KEY")

HEADERS = {
    "X-ApiKey": LODGY_API_KEY,
    "Content-Type": "application/json",
}

# ---------- Helpers (გადმოტანილია შენი მუშა კოდიდან) ----------
def extract_date_string(value):
    if isinstance(value, str):
        s = value.strip()
        if re.search(r"\d{4}-\d{2}-\d{2}", s):
            return s
        return s
    if isinstance(value, dict):
        for v in value.values():
            s = extract_date_string(v)
            if s:
                return s
    if isinstance(value, list):
        for v in value:
            s = extract_date_string(v)
            if s:
                return s
    return ""

def deep_find_preferred_date(obj, preferred_key_substrings):
    results = []
    def rec(o, path):
        if isinstance(o, dict):
            for k, v in o.items():
                kl = k.lower()
                if any(sub in kl for sub in preferred_key_substrings):
                    s = extract_date_string(v)
                    if s:
                        results.append(s)
                rec(v, path+[k])
        elif isinstance(o, list):
            for v in o:
                rec(v, path)
    rec(obj, [])
    return results[0] if results else ""

def extract_guest_info(booking):
    for key in ("guest","customer","contact","primaryGuest"):
        if key in booking and isinstance(booking[key], dict):
            g = booking[key]
            return g.get("first_name") or "", g.get("last_name") or "", g.get("email") or ""
    return "", "", ""

def normalize_list(payload):
    if isinstance(payload, list): return payload
    if isinstance(payload, dict):
        for k in ("items","bookings","results","data"):
            v = payload.get(k)
            if isinstance(v,list):
                return v
    return []

# ---------- Lodgify wrappers ----------
def fetch_properties():
    r = requests.get("https://api.lodgify.com/v1/properties", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_bookings():
    url = "https://api.lodgify.com/v2/reservations/bookings"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 200:
        return normalize_list(r.json())
    else:
        fb = requests.get("https://api.lodgify.com/v1/reservation", headers=HEADERS, timeout=30)
        return normalize_list(fb.json()) if fb.status_code==200 else []

# ---------- Monday wrappers ----------
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
    resp = requests.post("https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_TOKEN, "Content-Type":"application/json"},
        json={"query": query, "variables": {"board": BOARD_ID}})
    try:
        items = resp.json()["data"]["boards"][0]["items_page"]["items"]
        for it in items:
            for col in it["column_values"]:
                if col["id"]=="booking_id" and col["text"]==str(booking_id):
                    return it["id"]
    except Exception: pass
    return None

def upsert_booking(b):
    bid = b.get("id") or b.get("bookingId")
    g_first,g_last,email = extract_guest_info(b)
    guest = f"{g_first} {g_last}".strip() or "N/A"
    check_in = deep_find_preferred_date(b,["check_in","arrival","from","start"])
    check_out= deep_find_preferred_date(b,["check_out","departure","to","end"])
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

# ---------- Flask Routes ----------
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"

@app.route("/lodgify-webhook", methods=["POST"])
def lodgify_webhook():
    payload = request.json or {}
    result = upsert_booking(payload)
    return jsonify({"status":"ok","monday_response":result})

@app.route("/lodgify-sync-all", methods=["GET"])
def lodgify_sync_all():
    bookings = fetch_bookings()
    results=[]
    for b in bookings:
        results.append(upsert_booking(b))
    return jsonify({"status":"done","count":len(results),"sample":results[:3]})

@app.route("/lodgify-properties", methods=["GET"])
def lodgify_props():
    props = fetch_properties()
    return jsonify({"count":len(props),"sample":props[:3]})
