import os, json, logging, re, requests
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime
from flask import Flask, request, jsonify

# -----------------------
# Config / Logging
# -----------------------
app = Flask(__name__)
level = os.getenv("LOG_LEVEL", "INFO").upper()
handler = RotatingFileHandler("app.log", maxBytes=1_000_000, backupCount=3)
logging.basicConfig(level=level, handlers=[handler, logging.StreamHandler()])
log = logging.getLogger("lodgify-monday")

# -----------------------
# Helpers / Config Maps
# -----------------------
E164_RE = re.compile(r"^\+?[1-9]\d{6,14}$")

def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw)
    s = s.replace("(0)", "")
    s = re.sub(r"[\s\-().]", "", s)
    if s.startswith("00"):
        s = "+" + s[2:]
    if E164_RE.match(s):
        return s
    digits = re.sub(r"\D", "", s)
    return digits[-12:] if digits else ""

# Logical key → Monday column_id mapping (დაარეგულირე შენს ბორდზე)
COLUMN_MAP = {
    "reservation_id": "reservation_id",  # Text column (lookup)
    "unit": "text_unit",
    "email": "email",
    "phone": "phone",
    "check_in": "check_in",
    "check_out": "check_out",
    "total": "total",
    "currency": "currency",
    "status": "status_dropdown",         # Dropdown
    "assignee": "assignee",              # People
}

DROPDOWN_LABELS = {
    "status": {
        "confirmed": "Confirmed",
        "booked": "Confirmed",           # Lodgify v2 "Booked"
        "paid": "Paid",
        "pending": "Pending",
        "cancelled": "Cancelled",
        "canceled": "Cancelled",
        "default": "Pending",
    }
}

def to_date(v):
    if not v:
        return None
    # allow dicts like {"time": null, ...}
    if isinstance(v, dict):
        # best-effort: try v.get("time") or return None
        v = v.get("time") or None
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
    if col_id is not None and value is not None:
        cv[col_id] = value

# -----------------------
# Lodgify Client (v2)
# -----------------------
class LodgifyClient:
    def __init__(self, api_base: str, api_key: str):
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()
        # ვხმარობთ X-ApiKey ჰედერს (არა Bearer-ს)
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-ApiKey": self.api_key,
        })

    def list_bookings(self, limit: int = 50, skip: int = 0) -> List[dict]:
        url = f"{self.api_base}/v2/reservations/bookings"
        # სცენარი A: take/skip
        params = {"take": max(1, int(limit)), "skip": max(0, int(skip))}
        resp = self.session.get(url, params=params, timeout=45)

        # fallback სცენარი: page/pageSize
        if resp.status_code in (400, 404):
            page_size = max(1, int(limit))
            page_number = max(1, (int(skip) // page_size) + 1)
            params = {"pageSize": page_size, "pageNumber": page_number}
            resp = self.session.get(url, params=params, timeout=45)

        if not resp.ok:
            raise RuntimeError(f"Lodgify error {resp.status_code}: {resp.text[:500]}")

        data = resp.json() or {}
        items = data.get("results") or data.get("items") or data.get("data") or data
        if isinstance(items, dict):
            items = list(items.values())
        if not isinstance(items, list):
            items = []
        log.info("Lodgify v2 fetched %d item(s) (limit=%s, skip=%s)", len(items), limit, skip)
        return items

# -----------------------
# Monday Client
# -----------------------
@dataclass
class UpsertResult:
    ok: bool
    item_id: Optional[int] = None
    created: bool = False
    updated: bool = False
    error: Optional[str] = None

    def to_dict(self):
        return {
            "ok": self.ok,
            "item_id": self.item_id,
            "created": self.created,
            "updated": self.updated,
            "error": self.error,
        }

class MondayClient:
    def __init__(self, api_base: str, api_key: str, board_id: int):
        self.api_base = api_base
        self.api_key = api_key
        self.board_id = board_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": self.api_key,  # პირდაპირი ტოკენი (არა 'Bearer ...')
            "Content-Type": "application/json",
        })

    def _gql(self, query: str, variables: dict = None) -> dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        r = self.session.post(self.api_base, data=json.dumps(payload), timeout=45)
        if r.status_code != 200:
            raise RuntimeError(f"Monday HTTP {r.status_code}: {r.text[:500]}")
        out = r.json()
        if "errors" in out:
            raise RuntimeError(f"Monday GQL error: {out['errors']}")
        return out.get("data", {})

    def find_item_by_external_id(self, column_id: str, external_id: str) -> Optional[int]:
        # ID! ტიპებზე გადავიყვანეთ
        query = """
        query($board_id: ID!, $column_id: String!, $value: String!) {
          items_page_by_column_values(
            board_id: $board_id,
            columns: [{column_id: $column_id, column_values: [$value]}],
            limit: 1
          ) { items { id } }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "column_id": column_id, "value": external_id})
        items = (((data or {}).get("items_page_by_column_values") or {}).get("items")) or []
        return int(items[0]["id"]) if items else None

    def create_item(self, item_name: str, column_values: Dict[str, object]) -> int:
        query = """
        mutation($board_id: ID!, $name: String!, $cols: JSON!) {
          create_item(board_id: $board_id, item_name: $name, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"board_id": str(self.board_id), "name": item_name, "cols": json.dumps(column_values)})
        return int(data["create_item"]["id"])

    def update_item(self, item_id: int, column_values: Dict[str, object]) -> int:
        query = """
        mutation($item_id: ID!, $cols: JSON!) {
          change_multiple_column_values(item_id: $item_id, column_values: $cols) { id }
        }
        """
        data = self._gql(query, {"item_id": str(item_id), "cols": json.dumps(column_values)})
        return int(data["change_multiple_column_values"]["id"])

    def upsert_item(self, mapped: dict) -> UpsertResult:
        item_name = mapped["item_name"]
        external_id = mapped["external_id"]
        column_values = mapped["column_values"]

        lookup_col = COLUMN_MAP["reservation_id"]  # აუცილებელია ეს სვეტი არსებობდეს ბორდზე
        try:
            existing_id = self.find_item_by_external_id(lookup_col, external_id)
            if existing_id:
                self.update_item(existing_id, column_values)
                log.info("Updated Monday item id=%s (ext=%s)", existing_id, external_id)
                return UpsertResult(ok=True, item_id=existing_id, created=False, updated=True)
            else:
                new_id = self.create_item(item_name, column_values)
                log.info("Created Monday item id=%s (ext=%s)", new_id, external_id)
                return UpsertResult(ok=True, item_id=new_id, created=True, updated=False)
        except Exception as e:
            log.exception("Upsert failed for external_id=%s", external_id)
            return UpsertResult(ok=False, error=str(e))

# -----------------------
# Mapping
# -----------------------
def map_booking_to_monday(bk: dict) -> dict:
    # Lodgify v2 sample keys: guest.name, arrival, departure, currency_code, total_amount
    guest = bk.get("guest") or {}
    # name შეიძლება იყოს "First Last"
    full_name = (guest.get("name") or "").strip()
    first_name = (guest.get("first_name") or "").strip()
    last_name = (guest.get("last_name") or "").strip()
    if not (first_name or last_name):
        # სცადე full_name-ის გაყოფა
        if full_name:
            parts = full_name.split()
            if len(parts) == 1:
                first_name = parts[0]
            else:
                first_name = " ".join(parts[:-1])
                last_name = parts[-1]
    phone = normalize_phone(guest.get("phone") or guest.get("mobile") or "")
    email = guest.get("email") or ""

    res_id = str(bk.get("id") or bk.get("booking_id") or bk.get("code") or "")
    unit_name = (bk.get("rental") or {}).get("name") or bk.get("unit_name") or "Unknown unit"
    status = (bk.get("status") or "").lower()

    status_label = DROPDOWN_LABELS.get("status", {}).get(
        status,
        DROPDOWN_LABELS["status"].get("default", "Pending")
    )

    # Prefer v2 fields
    check_in_raw = bk.get("arrival") or bk.get("check_in")
    check_out_raw = bk.get("departure") or bk.get("check_out")
    check_in = to_date(check_in_raw)
    check_out = to_date(check_out_raw)

    total_price = bk.get("total_amount") or bk.get("total") or bk.get("price_total") or 0
    # ყურადღება: თუ ვალუტა არ მოიძებნა, ნაგულისხმევად იქნება "GBP". საჭიროების შემთხვევაში შეცვალეთ.
    currency = bk.get("currency_code") or bk.get("currency") or "GBP"

    display_name = (f"{first_name} {last_name}".strip() or full_name) or f"Booking {res_id}"

    cv = {}
    put(cv, "reservation_id", res_id)
    put(cv, "unit", unit_name)
    put(cv, "email", email)
    put(cv, "phone", phone)
    put(cv, "check_in", {"date": check_in})
    put(cv, "check_out", {"date": check_out})
    put(cv, "total", total_price)
    put(cv, "currency", currency)
    put(cv, "status", {"labels": [status_label]})

    # --- [შესწორება] ---
    # ქვემოთ მოცემული ლოგიკა იწვევდა შეცდომას, რადგან Monday.com-ის "People" სვეტი
    # `id`-ში ელოდება მომხმარებლის ციფრულ ID-ს და არა ელფოსტას.
    # ამ ფუნქციონალის სწორად ასამუშავებლად საჭიროა დამატებითი ლოგიკა,
    # რომელიც ელფოსტით მოძებნის მომხმარებლის ID-ს Monday-ს ბაზაში.
    #
    # if email:
    #     cv[COLUMN_MAP["assignee"]] = {"personsAndTeams": [{"id": email, "kind": "person"}]}

    return {"item_name": display_name, "external_id": res_id, "column_values": cv}

# -----------------------
# Flask endpoints
# -----------------------
LODGY_API_BASE = os.getenv("LODGY_API_BASE", "https://api.lodgify.com")
LODGY_API_KEY  = os.getenv("LODGY_API_KEY", "")
MONDAY_API_BASE = os.getenv("MONDAY_API_BASE", "https://api.monday.com/v2")
MONDAY_API_KEY  = os.getenv("MONDAY_API_KEY", "")

# --- [შესწორება] ---
# დავამატეთ try-except ბლოკი, რათა აპლიკაცია არ გაითიშოს,
# თუ MONDAY_BOARD_ID ცვლადი არასწორად არის მითითებული.
try:
    MONDAY_BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "0"))
except (ValueError, TypeError):
    log.error("MONDAY_BOARD_ID გარემოს ცვლადი არასწორია. გამოყენებულია ნაგულისხმევი ID: 0")
    MONDAY_BOARD_ID = 0

lodgify = LodgifyClient(api_base=LODGY_API_BASE, api_key=LODGY_API_KEY)
monday  = MondayClient(api_base=MONDAY_API_BASE, api_key=MONDAY_API_KEY, board_id=MONDAY_BOARD_ID)

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "lodgify-monday", "board_id": MONDAY_BOARD_ID}), 200

@app.get("/")
def root():
    return jsonify({"ok": True, "endpoints": ["/health", "/lodgify-sync-all", "/webhook/lodgify"]}), 200

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
                results.append({"ok": False, "error": str(e), "source_id": bk.get("id")})
        response = {"ok": True, "count": len(results), "results": results}
        if debug and bookings:
            response["sample_input"] = bookings[:1]
            response["sample_mapped"] = map_booking_to_monday(bookings[0])
        return jsonify(response), 200
    except Exception as e:
        log.exception("Batch sync failed")
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
