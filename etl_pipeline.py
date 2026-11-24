import json
import os 
import re
from datetime import datetime
import psycopg2
import requests
from dotenv import load_dotenv

#Extraction
API_URL = "https://jsonplaceholder.typicode.com/users"

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

#Download and return as a list
def extract():
    print(f"\n[EXTRACT] GET {API_URL}")

    response = requests.get(API_URL, timeout=10)
    print(f"[EXTRACT] status code: {response.status_code}")
    response.raise_for_status()
    
    raw_data = response.json()

    if not isinstance(raw_data, list) or len(raw_data) == 0:
        raise ValueError("The API returned no users")

    print(f"[EXTRACT] Received {len(raw_data)} raw record")
    print(f"[EXTRACT] First raw record, exactly as received: ")
    print(json.dumps(raw_data[0], indent=2))
    return raw_data

    

# if __name__ == "__main__":
#     raw_data = extract()


#Transformation
def clean_text(value):
    if value is None:
        return None
    value = " ".join(str(value).split())
    return value or None

# def clean_phone(value):
#     if value is None:
#         return None
#     value = str(value).strip()

#     if not value:
#         return None
#     value = re.sub(
#         r'\s*(?:ext\.?|extension|x)\s*\d+$',
#         '',
#         value,
#         flags=re.IGNORECASE
#     )

#     digits = re.sub(r'\D', '', value)

#     if len(digits) == 11 and digits.startswith('1'):
#         digits = digits[1:]

#     if len(digits) < 7 or len(digits) > 15:
#         return None

#     return digits

# def clean_phone(value):
#     """Cleans phone numbers by keeping only digits, spaces, and standard phone symbols."""
#     if value is None:
#         return None
#     # Remove everything except digits, plus sign, parentheses, hyphens, and spaces
#     cleaned = re.sub(r"[^\d\+\-\(\)\s]", "", str(value))
#     cleaned = " ".join(cleaned.split()).strip() # Normalize whitespace
#     return cleaned or None


# def clean_phone(value):
#     if value is None:
#         return None

#     # Remove extension (anything from 'x' or 'ext' onward)
#     without_ext = re.split(r"\s*(?:x|ext\.?)\s*", str(value).strip(), maxsplit=1, flags=re.IGNORECASE)[0]

#     # Remove all non-digit characters (hyphens, dots, parentheses, spaces, +)
#     digits = re.sub(r"\D", "", without_ext)

#     return digits or None


def clean_phone(value):
    if value is None:
        return None
    number = str(value).lower().split("x")[0]
    digits = ""
    for character in number:
        if character.isdigit():
            digits += character
    return digits or None



def transform(raw_data):
    print("\n[TRANSFORM] Cleaning records...")
    clean_users = []

    for user in raw_data:
        address = user.get("address") or {}
        record = {
            "user_id": user.get("id"),
            "full_name": clean_text(user.get("name")),
            "username": clean_text(user.get("username")),
            "email": clean_text(user.get("email")),
            "phone": clean_phone(user.get("phone")),
            "city": clean_text(address.get("city")) or "unknown",
            "website": clean_text(user.get("website"))
        }
        #validating data
        if record["user_id"] is None or record["full_name"] is None:
            print(f"[TRANSFORM] skipped user {record["user_id"]}: missing id or name")
            continue
        if record["email"] is None or "@" not in record["email"]:
            print(f"[TRANSFORM] skipped user {record["email"]}: invalid email")
            continue
        record["email"] = record["email"].lower()
        clean_users.append(record)

    print(f"[TRANSFORM] {len(clean_users)} clean records")
    return clean_users


#Running the extraction step
# if __name__ == "__main__":
#     raw_data = extract()
#     clean_users = transform(raw_data)
#     print(clean_users[0])


#Loading data
#Creating a SQL table to load data
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
user_id INTEGER PRIMARY KEY,
full_name TEXT NOT NULL,
username TEXT,
email TEXT NOT NULL,
phone TEXT,
city TEXT,
website TEXT,
loaded_at TIMESTAMP DEFAULT NOW()
);
"""

#Upsert (Insert new users and updataing usres incase they already exist on the database)
UPSERT_SQL = """
INSERT INTO users (user_id, full_name, username, email, phone, city, website)
VALUES (%(user_id)s, %(full_name)s, %(username)s, %(email)s, %(phone)s, %(city)s, %(website)s)
ON CONFLICT (user_id) DO UPDATE SET
full_name = EXCLUDED.full_name,
username = EXCLUDED.username,
email = EXCLUDED.email,
phone = EXCLUDED.phone,
city = EXCLUDED.city,
website = EXCLUDED.website,
loaded_at = NOW();
"""


def load(clean_users):
    print("\n[LOAD] Connecting to postgresql...")
    conn = psycopg2.connect(DATABASE_URL)
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                cur.executemany(UPSERT_SQL, clean_users)
        print(f"[LOAD] {len(clean_users)} rows updated or rows inserted in table 'users'")
    finally:
        conn.close()


if __name__ == "__main__":
    raw_data = extract()
    clean_users = transform(raw_data)
    print(clean_users)
    load(clean_users)