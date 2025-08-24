import requests
import json
import re
from datetime import datetime as dt

# --- ჩასვი შენი რეალური API key ---
API_KEY = "RkOMVg6VquJSNuVUxFbjpVQE+wFMVSodwxT5Nq+dIbiNScqzaJPKkX2npLF1/ROV"

HEADERS = {
    "X-ApiKey": API_KEY,             # Lodgify API header
    "Content-Type": "application/json",
}

# ---------- Helpers ----------
def extract_date_string(value):
    """
    აბრუნებს თარიღის string-ს value-დან (როდესაც value შეიძლება იყოს string/dict/list).
    ეძებს ISO/YYYY-MM-DD/სხვ. ფორმატებს.
    """
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return ""
        if re.search(r"\d{4}-\d{2}-\d{2}", s):
            return s
        if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
            return s
        if "T" in s and re.search(r"\d{4}-\d{2}-\d{2}T", s):
            return s
        return s  # უკან ვაბრუნებთ მაინც; მერე parse ვერ თუ გავაგრძელებთ

    if isinstance(value, dict):
        common_keys = [
            "check_in_date", "checkInDate", "check_in", "checkIn",
            "arrival", "date", "date_utc", "dateUtc", "dateTime", "datetime",
            "localDate", "local_date", "iso", "value", "from", "start",
        ]
        nested_candidates = [
            ["date", "value"],
            ["date", "iso"],
            ["from", "date"],
            ["start", "date"],
            ["arrival", "date"],
        ]
        for k in common_keys:
            if k in value:
                s = extract_date_string(value[k])
                if s:
                    return s
        for path in nested_candidates:
            cur = value
            ok = True
            for kk in path:
                if isinstance(cur, dict) and kk in cur:
                    cur = cur[kk]
                else:
                    ok = False
                    break
            if ok:
                s = extract_date_string(cur)
                if s:
                    return s
        # fallback: ღრმა ძიება
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
    """
    მთელ booking ობიექტში ღრმად ეძებს ველებს, რომლების სახელშიც მოცემული keywords ერთ-ერთია.
    პოულობს პირველ ვალიდურ string თარიღს.
    """
    results = []

    def rec(o, path):
        if isinstance(o, dict):
            for k, v in o.items():
                kl = k.lower()
                match_idx = None
                for idx, sub in enumerate(preferred_key_substrings):
                    if sub in kl:
                        match_idx = idx
                        break
                if match_idx is not None:
                    s = extract_date_string(v)
                    if s:
                        results.append((match_idx, len(path), path + [k], s))
                rec(v, path + [k])
        elif isinstance(o, list):
            for i, v in enumerate(o):
                rec(v, path + [f"[{i}]"])

    rec(obj, [])
    if not results:
        return ""
    results.sort(key=lambda t: (t[0], t[1]))  # keyword-ის პრიორიტეტი, მერე ბილიკის სიგრძე
    return results[0][3]

def deep_iter_dicts(obj, key_targets):
    """რეკურსიულად აგროვებს dict-ებს, რომელთა parent key ერთ-ერთია key_targets-იდან."""
    results = []
    def rec(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in key_targets and isinstance(v, dict):
                    results.append(v)
                rec(v)
        elif isinstance(o, list):
            for v in o:
                rec(v)
    rec(obj)
    return results

def extract_guest_info(booking):
    """იღებს სტუმრის first/last/email-ს სხვადასხვა შესაძლო სტრუქტურიდან."""
    key_targets = {"guest", "customer", "contact", "primaryGuest", "lead", "booker", "tenant"}
    candidates = deep_iter_dicts(booking, key_targets)
    first = last = email = ""
    for cand in candidates:
        email = email or cand.get("email", "") or cand.get("eMail", "") or cand.get("mail", "")
        first = first or cand.get("first_name", "") or cand.get("firstName", "")
        last  = last  or cand.get("last_name", "")  or cand.get("lastName", "")
        full  = cand.get("full_name", "") or cand.get("fullName", "") or cand.get("name", "")
        if (not first or not last) and full:
            parts = str(full).strip().split()
            if parts:
                if not first:
                    first = parts[0]
                if len(parts) > 1 and not last:
                    last = " ".join(parts[1:])
        if first or last or email:
            break
    if not email:
        email = booking.get("email", "")
    return first.strip(), last.strip(), email.strip()

def parse_date_for_sort(s):
    if not s:
        return dt.max
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return dt.strptime(s, fmt)
        except Exception:
            continue
    return dt.max

def normalize_list(payload):
    """აბრუნებს list-ს; ზოგჯერ მოდის {'items': [...]}, {'bookings': [...]}, {'results': [...]}, {'data': [...]}"""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "bookings", "results", "data"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
    return []

# ---------- API wrappers ----------
def fetch_properties():
    url = "https://api.lodgify.com/v1/properties"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    props = r.json()
    id_to_name_int = {}
    id_to_name_str = {}
    for p in props:
        pid = p.get("id")
        pname = p.get("name")
        if pid is not None:
            id_to_name_int[pid] = pname
            id_to_name_str[str(pid)] = pname
    return props, id_to_name_int, id_to_name_str

def fetch_bookings_v2():
    url = "https://api.lodgify.com/v2/reservations/bookings"
    return requests.get(url, headers=HEADERS, timeout=30)

def fetch_inbox_v1():
    url = "https://api.lodgify.com/v1/reservation"
    return requests.get(url, headers=HEADERS, timeout=30)

# ---------- Printing ----------
def print_properties(props):
    print("\n--- Properties (v1) ---")
    for p in props:
        print(f"{p.get('id')} | {p.get('name')}")

def print_bookings_v2(bookings, id_to_name_int, id_to_name_str):
    rows = []
    for b in bookings:
        # booking id
        bid = (b.get("id")
               or b.get("booking_id")
               or b.get("bookingId"))

        # property id/name
        pid = (b.get("property_id")
               or b.get("propertyId")
               or (b.get("property", {}) if isinstance(b.get("property"), dict) else None))
        if isinstance(pid, dict):
            pname = pid.get("name")
            pid = pid.get("id") or pid.get("property_id") or pid.get("propertyId")
        else:
            pname = None

        # resolve property name
        pname = (pname
                 or id_to_name_int.get(pid)
                 or id_to_name_str.get(str(pid))
                 or str(pid) if pid is not None else "N/A")

        # guest info
        g_first, g_last, email = extract_guest_info(b)
        guest_display = (f"{g_first} {g_last}".strip() if (g_first or g_last) else "N/A")

        # dates: ინფოს ღრმა ძიება (check-in / check-out)
        check_in = deep_find_preferred_date(
            b, ["check_in", "checkin", "arrival", "start", "from"]
        )
        check_out = deep_find_preferred_date(
            b, ["check_out", "checkout", "departure", "end", "to"]
        )

        rows.append({
            "booking_id": bid,
            "property_name": pname,
            "guest": guest_display,
            "email": email,
            "check_in": check_in,
            "check_out": check_out,
            "_sort": parse_date_for_sort(check_in),
            "_raw": b,
        })

    rows.sort(key=lambda r: r["_sort"])

    print("\n--- Bookings (v2) ---")
    if not rows:
        print("(empty)")
        return

    any_dates = False
    for r in rows:
        if r["check_in"] or r["check_out"]:
            any_dates = True
        print(f"#{r['booking_id']} | {r['property_name']} | {r['guest']} | {r['check_in']} → {r['check_out']} | {r['email']}")

    # თუ ვერსად ვნახეთ თარიღები, ამოაგდო ნედლი მაგალითი დასაკვირვებლად
    if not any_dates:
        sample = rows[0]["_raw"]
        print("\n[DEBUG] No date fields detected. Raw sample of the first booking:")
        try:
            print(json.dumps(sample, indent=2))
        except Exception:
            print(sample)

# ---------- Main ----------
def main():
    # Properties
    try:
        props, id_to_name_int, id_to_name_str = fetch_properties()
        print_properties(props)
    except Exception as e:
        print("Failed to fetch properties:", e)
        id_to_name_int, id_to_name_str = {}, {}

    # Bookings v2 (fallback v1 inbox თუ ვერ იმუშავებს)
    try:
        r = fetch_bookings_v2()
        if r.status_code == 200:
            try:
                payload = r.json()
            except Exception:
                print("\n/v2/reservations/bookings returned non-JSON:")
                print(r.text)
                payload = None

            if payload is not None:
                bookings = normalize_list(payload)
                print_bookings_v2(bookings, id_to_name_int, id_to_name_str)
            else:
                print("\nEmpty payload from v2.")
        else:
            print(f"\n/v2/reservations/bookings → {r.status_code}. Body:\n{r.text}")
            fb = fetch_inbox_v1()
            if fb.status_code == 200:
                try:
                    inbox_payload = fb.json()
                except Exception:
                    print("\n/v1/reservation returned non-JSON:")
                    print(fb.text)
                    inbox_payload = None
                if inbox_payload is not None:
                    print("\n--- Inbox (v1) bookings/enquiries — raw dump ---")
                    print(json.dumps(inbox_payload, indent=2))
            else:
                print(f"/v1/reservation → {fb.status_code}. Body:\n{fb.text}")
    except Exception as e:
        print("Failed to fetch bookings:", e)

if __name__ == "__main__":
    main()
