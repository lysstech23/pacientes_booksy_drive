#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script to extract patient details from Booksy booking emails and write them into a Google Sheet.

This script connects to Gmail and searches for messages from the sender `no-reply@booksy.com`. For each email,
it extracts the patient's first name, last name, telephone and email address. The information is appended to a
Google Sheet named ``directorio_pacientes``. To avoid reprocessing the same emails, the Gmail message ID and
processing timestamp are stored in a meta sheet. Duplicate entries (same phone or email) are not added.

Features:

  * **Initial run** processes the complete history of the specified sender.
  * **Recurring runs** only process unseen messages.
  * **Deduplication** based on Gmail message ID and patient email/telephone.
  * **Flexible parsing** to handle different Booksy email formats.
  * **Configurable** via environment variables (client credentials, spreadsheet ID).

Usage:
    python src/booksy_gmail_to_sheets.py

Environment Variables:

    GOOGLE_CLIENT_ID:         OAuth client ID (for CI mode)
    GOOGLE_CLIENT_SECRET:     OAuth client secret (for CI mode)
    GOOGLE_REFRESH_TOKEN:     OAuth refresh token (for CI mode)
    GOOGLE_ACCESS_TOKEN:      Optional, existing access token (will be refreshed if expired)
    GOOGLE_SHEETS_SPREADSHEET_ID: ID of an existing spreadsheet (optional, will create if missing)
    GOOGLE_SHEETS_TITLE:      Title of the spreadsheet to create if ID is not provided (default: "directorio_pacientes")

Local Mode:
    Provide a ``credentials.json`` OAuth client file in the working directory.
    The first run will prompt a browser window to authorize. A ``token.json`` will be created for subsequent runs.

Dependencies:
    google-api-python-client
    google-auth
    google-auth-oauthlib
    beautifulsoup4

"""

import base64
import os
import re
import sys
import time
import json
import datetime as dt
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from bs4 import BeautifulSoup


GMAIL_QUERY = 'from:no-reply@booksy.com'
SHEET_TITLE = os.getenv("GOOGLE_SHEETS_TITLE", "directorio_pacientes")
META_SHEET = "_meta_processed_messages"
DATA_SHEET = "directorio_pacientes"
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]


def _creds_from_env() -> Optional[Credentials]:
    """Create OAuth credentials from environment variables."""
    cid = os.getenv("GOOGLE_CLIENT_ID")
    cs = os.getenv("GOOGLE_CLIENT_SECRET")
    rt = os.getenv("GOOGLE_REFRESH_TOKEN")
    token = os.getenv("GOOGLE_ACCESS_TOKEN")
    if cid and cs and rt:
        data = {
            "token": token or "",
            "refresh_token": rt,
            "client_id": cid,
            "client_secret": cs,
            "scopes": SCOPES,
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        creds = Credentials.from_authorized_user_info(data)
        if not creds.valid and creds.refresh_token:
            creds.refresh(Request())
        return creds
    return None


def _creds_local() -> Credentials:
    """Create OAuth credentials for local runs using credentials.json/token.json."""
    token_path = "token.json"
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                print("ERROR: Missing credentials.json for local mode.", file=sys.stderr)
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds


def get_creds() -> Credentials:
    """Load credentials either from environment or via local OAuth flow."""
    creds = _creds_from_env()
    if creds:
        return creds
    return _creds_local()


def gmail_service(creds: Credentials):
    """Initialize Gmail API service."""
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def list_message_ids(service, user_id: str, query: str) -> List[str]:
    """List all message IDs matching the given Gmail query."""
    ids: List[str] = []
    page_token: Optional[str] = None
    while True:
        resp = (
            service.users()
            .messages()
            .list(userId=user_id, q=query, pageToken=page_token, maxResults=500)
            .execute()
        )
        for m in resp.get("messages", []):
            ids.append(m["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return ids


def get_message_full(service, user_id: str, msg_id: str) -> Dict:
    """Get the full message including payload and internalDate."""
    return (
        service.users().messages().get(userId=user_id, id=msg_id, format="full").execute()
    )


def decode_body(payload: Dict) -> str:
    """
    Decode the email payload into plain text.

    Prefers HTML parts converted to text via BeautifulSoup, falls back to text/plain parts.
    """
    parts: List[str] = []

    def _walk(part: Dict):
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data")
        if part.get("parts"):
            for sp in part["parts"]:
                _walk(sp)
        else:
            if data:
                try:
                    raw = base64.urlsafe_b64decode(data.encode("utf-8"))
                except Exception:
                    raw = base64.b64decode(data)
                if mime == "text/html":
                    soup = BeautifulSoup(raw, "html.parser")
                    text = soup.get_text("\n")
                    parts.append(text)
                elif mime == "text/plain":
                    parts.append(raw.decode("utf-8", errors="ignore"))

    _walk(payload)
    if not parts:
        body = payload.get("body", {}).get("data")
        if body:
            raw = base64.urlsafe_b64decode(body.encode("utf-8"))
            try:
                soup = BeautifulSoup(raw, "html.parser")
                parts.append(soup.get_text("\n"))
            except Exception:
                parts.append(raw.decode("utf-8", errors="ignore"))
    text = "\n".join([p.strip() for p in parts if p and p.strip()])
    text = re.sub(r"[ \t]+\n", "\n", text)
    return text


EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?34[\s\-]?)?(?:\d[\s\-]?){9,13}")


def normalize_phone(s: str) -> str:
    """Normalize phone number: remove non-digits and ensure Spanish prefix if missing."""
    s2 = re.sub(r"[^\d+]", "", s)
    if s2.startswith("+"):
        return s2
    digits = re.sub(r"\D", "", s2)
    if len(digits) == 9:
        return "+34" + digits
    return s2


def guess_name_lines(text: str, email_found: Optional[str], phone_found: Optional[str]) -> str:
    """Heuristic to guess the patient's full name from the email body."""
    m = re.search(r"¡\s*([^\n:]+?)\s*:\s*nueva\s+reserva", text, flags=re.I)
    if m:
        return m.group(1).strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    indices = []
    if email_found:
        for i, ln in enumerate(lines):
            if email_found in ln:
                indices.append(i)
    if phone_found:
        for i, ln in enumerate(lines):
            if re.sub(r"\s+", "", phone_found) in re.sub(r"\s+", "", ln):
                indices.append(i)
    candidates = []
    for idx in indices or [0]:
        for j in range(max(0, idx - 3), min(len(lines), idx + 3)):
            ln = lines[j]
            if EMAIL_RE.search(ln):
                continue
            if PHONE_RE.search(ln):
                continue
            if re.search(r"\b€|\bEUR|\d{1,2}:\d{2}", ln):
                continue
            tokens = ln.split()
            if 1 <= len(tokens) <= 5:
                cap_score = sum(
                    1 for t in tokens if re.match(r"^[A-ZÁÉÍÓÚÑ][a-záéíóúñü]+$", t)
                )
                if cap_score >= max(1, len(tokens) // 2):
                    candidates.append((j, ln))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    for ln in lines[:15]:
        tokens = ln.split()
        if (
            1 <= len(tokens) <= 5
            and not EMAIL_RE.search(ln)
            and not PHONE_RE.search(ln)
        ):
            return ln
    return ""


def split_name(full_name: str) -> Tuple[str, str]:
    """Split full name into first name and surnames."""
    full_name = re.sub(r"\s+", " ", full_name).strip()
    if not full_name:
        return "", ""
    parts = full_name.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def parse_patient(text: str) -> Dict[str, str]:
    """Extract patient data from the body text."""
    email_match = EMAIL_RE.search(text)
    phone_match = PHONE_RE.search(text)
    email = email_match.group(0).strip() if email_match else ""
    phone_raw = phone_match.group(0).strip() if phone_match else ""
    phone = normalize_phone(phone_raw) if phone_raw else ""
    name_line = guess_name_lines(text, email, phone)
    nombre, apellidos = split_name(name_line)
    return {
        "nombre": nombre,
        "apellidos": apellidos,
        "telefono": phone,
        "email": email,
    }


def sheets_service(creds: Credentials):
    """Initialize Google Sheets API service."""
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_or_create_spreadsheet_id(svc, title: str) -> str:
    ssid = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if ssid:
        return ssid
    body = {"properties": {"title": title}}
    resp = svc.spreadsheets().create(body=body, fields="spreadsheetId").execute()
    return resp["spreadsheetId"]


def ensure_sheets_and_headers(svc, spreadsheet_id: str):
    """Ensure that the data and meta sheets exist with appropriate headers."""
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = {sh["properties"]["title"] for sh in meta.get("sheets", [])}
    requests: List[dict] = []

    def add_sheet(title: str):
        requests.append({"addSheet": {"properties": {"title": title}}})

    if DATA_SHEET not in titles:
        add_sheet(DATA_SHEET)
    if META_SHEET not in titles:
        add_sheet(META_SHEET)
    if requests:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()

    def write_headers(sheet: str, headers: List[str]):
        rng = f"{sheet}!A1:{chr(64 + len(headers))}1"
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

    def is_empty(sheet: str) -> bool:
        rng = f"{sheet}!A1:A1"
        resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=rng
        ).execute()
        return not resp.get("values")

    if is_empty(DATA_SHEET):
        write_headers(
            DATA_SHEET,
            [
                "Nombre",
                "Apellidos",
                "Telefono",
                "Email",
                "Gmail_Message_ID",
                "Gmail_Date",
            ],
        )
    if is_empty(META_SHEET):
        write_headers(META_SHEET, ["Gmail_Message_ID", "Processed_At"])


def read_set_from_col(svc, spreadsheet_id: str, sheet: str, col_letter: str) -> set:
    """Read unique values from a column as a set."""
    rng = f"{sheet}!{col_letter}2:{col_letter}"
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    vals = resp.get("values", [])
    return {row[0].strip() for row in vals if row and row[0].strip()}


def read_processed_ids(svc, spreadsheet_id: str) -> set:
    return read_set_from_col(svc, spreadsheet_id, META_SHEET, "A")


def append_rows(svc, spreadsheet_id: str, sheet: str, rows: List[List[str]]):
    """Append rows to a Google sheet."""
    if not rows:
        return
    rng = f"{sheet}!A:A"
    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def process():
    """Main processing function."""
    creds = get_creds()
    gmail = gmail_service(creds)
    sheets = sheets_service(creds)
    ssid = get_or_create_spreadsheet_id(sheets, SHEET_TITLE)
    ensure_sheets_and_headers(sheets, ssid)
    processed_ids = read_processed_ids(sheets, ssid)
    existing_emails = read_set_from_col(sheets, ssid, DATA_SHEET, "D")
    existing_phones = read_set_from_col(sheets, ssid, DATA_SHEET, "C")
    msg_ids = list_message_ids(gmail, "me", GMAIL_QUERY)
    rows_data: List[List[str]] = []
    rows_meta: List[List[str]] = []
    for mid in msg_ids:
        if mid in processed_ids:
            continue
        try:
            msg = get_message_full(gmail, "me", mid)
            payload = msg.get("payload", {})
            text = decode_body(payload)
            patient = parse_patient(text)
            nombre = patient.get("nombre", "").strip()
            apellidos = patient.get("apellidos", "").strip()
            telefono = patient.get("telefono", "").strip()
            email = patient.get("email", "").strip()
            key_exists = False
            if email and email in existing_emails:
                key_exists = True
            if telefono and telefono in existing_phones:
                key_exists = True
            internal_date = msg.get("internalDate")
            gmail_date_iso = ""
            if internal_date:
                try:
                    ts = int(internal_date) / 1000.0
                    gmail_date_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z"
                except Exception:
                    gmail_date_iso = ""
            if not key_exists and (email or telefono or nombre or apellidos):
                rows_data.append(
                    [
                        nombre,
                        apellidos,
                        telefono,
                        email,
                        mid,
                        gmail_date_iso,
                    ]
                )
                if email:
                    existing_emails.add(email)
                if telefono:
                    existing_phones.add(telefono)
            now_iso = dt.datetime.utcnow().isoformat() + "Z"
            rows_meta.append([mid, now_iso])
            if len(rows_meta) >= 200:
                append_rows(sheets, ssid, DATA_SHEET, rows_data)
                append_rows(sheets, ssid, META_SHEET, rows_meta)
                rows_data.clear()
                rows_meta.clear()
        except HttpError as e:
            print(f"Gmail API error on {mid}: {e}", file=sys.stderr)
            time.sleep(1)
            continue
        except Exception as ex:
            print(f"Error processing {mid}: {ex}", file=sys.stderr)
            continue
    append_rows(sheets, ssid, DATA_SHEET, rows_data)
    append_rows(sheets, ssid, META_SHEET, rows_meta)


if __name__ == "__main__":
    process()
