#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Booksy → Gmail → Google Sheets/Drive + export a repo outputs/.

Funciones clave:
- Lee Gmail (from:no-reply@booksy.com), parsea Nombre/Apellidos/Teléfono/Email con heurísticas robustas.
- Evita duplicados por Gmail Message-ID, email y teléfono.
- Escribe/actualiza Google Sheet 'directorio_pacientes' (en carpeta Drive 'Automatizaciones-no-tocar').
- Ordena por Nombre.
- Normaliza capitalización de nombres y apellidos.
- Exporta a GitHub workspace: outputs/pacs_contacts.csv y outputs/pacs_contacts_index.json.
- Reintenta ante fallos SSL/5xx en Google API (SSLEOFError).

Requiere:
- Variables de entorno:
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
  (y opcional: GOOGLE_SHEETS_SPREADSHEET_ID)
"""

import os
import re
import csv
import json
import ssl
import time
import base64
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# === Config ===
BOOKSY_SENDER = "no-reply@booksy.com"
SHEET_TITLE = "directorio_pacientes"
DATA_SHEET = "directorio_pacientes"
META_SHEET = "_meta_processed_messages"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# === Exec retry helper ===
def exec_retry(req, retries: int = 6, base_delay: float = 1.0):
    delay = base_delay
    last = None
    for _ in range(retries):
        try:
            return req.execute()
        except Exception as e:
            last = e
            if isinstance(e, HttpError) and getattr(e, "resp", None) and 500 <= e.resp.status < 600:
                time.sleep(delay)
                delay *= 2
                continue
            if isinstance(e, ssl.SSLEOFError) or "SSLEOFError" in repr(e):
                time.sleep(delay)
                delay *= 2
                continue
            if "Connection reset" in repr(e) or "Broken pipe" in repr(e):
                time.sleep(delay)
                delay *= 2
                continue
            raise
    return req.execute()

# === Credenciales y servicios ===
def _creds_from_env() -> Credentials:
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Faltan GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN.")
    return Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )

def get_creds() -> Credentials:
    creds = _creds_from_env()
    if not creds.valid:
        creds.refresh(Request())
    return creds

def gmail_service(creds: Credentials):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def sheets_service(creds: Credentials):
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def drive_service(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

# === Drive y Sheets helpers ===
def get_or_create_folder_id(drive, folder_name="Automatizaciones-no-tocar"):
    q = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    resp = exec_retry(drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=5))
    items = resp.get("files", [])
    if items:
        return items[0]["id"]
    meta = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
    folder = exec_retry(drive.files().create(body=meta, fields="id"))
    return folder["id"]

def move_file_to_folder(drive, file_id: str, folder_id: str):
    file_info = exec_retry(drive.files().get(fileId=file_id, fields="parents"))
    prev_parents = ",".join(file_info.get("parents", []))
    exec_retry(
        drive.files().update(fileId=file_id, addParents=folder_id, removeParents=prev_parents, fields="id, parents")
    )

def get_or_create_spreadsheet_id(svc, title: str, drive=None) -> str:
    ssid = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if ssid:
        return ssid
    if drive:
        q = f"name = '{title}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
        resp = exec_retry(drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=5))
        items = resp.get("files", [])
        if items:
            return items[0]["id"]
    created = exec_retry(svc.spreadsheets().create(body={"properties": {"title": title}}, fields="spreadsheetId"))
    ssid = created["spreadsheetId"]
    if drive:
        folder_id = get_or_create_folder_id(drive)
        move_file_to_folder(drive, ssid, folder_id)
    return ssid

def sheet_exists(svc, spreadsheet_id: str, title: str) -> bool:
    meta = exec_retry(svc.spreadsheets().get(spreadsheetId=spreadsheet_id))
    return any(s.get("properties", {}).get("title") == title for s in meta.get("sheets", []))

def ensure_sheets_and_headers(svc, spreadsheet_id: str):
    requests = []
    for sheet in [DATA_SHEET, META_SHEET]:
        if not sheet_exists(svc, spreadsheet_id, sheet):
            requests.append({"addSheet": {"properties": {"title": sheet}}})
    if requests:
        exec_retry(svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}))

    def set_headers(title, headers):
        rng = f"{title}!A1:{chr(64 + len(headers))}1"
        exec_retry(
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=rng, valueInputOption="RAW", body={"values": [headers]}
            )
        )

    set_headers(DATA_SHEET, ["Nombre", "Apellidos", "Telefono", "Email", "Gmail_Message_ID", "Gmail_Date"])
    set_headers(META_SHEET, ["Gmail_Message_ID"])

def read_processed_ids(svc, spreadsheet_id: str) -> set:
    resp = exec_retry(svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{META_SHEET}!A2:A"))
    vals = resp.get("values", [])
    return {r[0] for r in vals if r}

def read_set_from_col(svc, spreadsheet_id: str, sheet_title: str, col_letter: str) -> set:
    resp = exec_retry(svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_title}!{col_letter}2:{col_letter}"))
    vals = resp.get("values", [])
    out = set()
    for row in vals:
        if not row:
            continue
        value = str(row[0]).strip()
        if not value:
            continue
        if col_letter.upper() == "C":
            value = value.replace(" ", "")
        out.add(value)
    return out

def append_rows(svc, spreadsheet_id: str, sheet_title: str, rows: List[List[str]]):
    if not rows:
        return
    exec_retry(
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_title}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        )
    )

def sort_data_sheet_by_name(svc, spreadsheet_id: str, sheet_title: str):
    meta = exec_retry(svc.spreadsheets().get(spreadsheetId=spreadsheet_id))
    sheet_id = next(
        (s["properties"]["sheetId"] for s in meta["sheets"] if s["properties"]["title"] == sheet_title), None
    )
    if sheet_id is None:
        return
    exec_retry(
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "sortRange": {
                            "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 0},
                            "sortSpecs": [{"dimensionIndex": 0, "sortOrder": "ASCENDING"}],
                        }
                    }
                ]
            },
        )
    )

def export_patients_to_repo(svc, spreadsheet_id: str, sheet_title=DATA_SHEET):
    os.makedirs("outputs", exist_ok=True)
    resp = exec_retry(
        svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{sheet_title}!A1:F", valueRenderOption="UNFORMATTED_VALUE"
        )
    )
    rows = resp.get("values", []) or []
    if not rows:
        return

    csv_path = os.path.join("outputs", "pacs_contacts.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)

    header = rows[0]
    idx = {h.lower(): i for i, h in enumerate(header)}
    def col(name): return idx.get(name, -1)

    data = {}
    for r in rows[1:]:
        if not r:
            continue
        tel = str(r[col("telefono")]).replace(" ", "") if col("telefono") >= 0 else ""
        if not tel:
            continue
        data[tel] = {
            "nombre": r[col("nombre")] if col("nombre") >= 0 and col("nombre") < len(r) else "",
            "apellidos": r[col("apellidos")] if col("apellidos") >= 0 and col("apellidos") < len(r) else "",
            "email": r[col("email")] if col("email") >= 0 and col("email") < len(r) else "",
            "gmail_id": r[col("gmail_message_id")] if col("gmail_message_id") >= 0 and col("gmail_message_id") < len(r) else "",
            "fecha": r[col("gmail_date")] if col("gmail_date") >= 0 and col("gmail_date") < len(r) else "",
        }

    json_path = os.path.join("outputs", "pacs_contacts_index.json")
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(data, jf, ensure_ascii=False, indent=2)

# === Parsing ===
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?34[\s\-]?)?(?:\d[\s\-]?){9,13}")

def normalize_phone(s: str) -> str:
    s2 = re.sub(r"[^\d+]", "", s or "")
    if s2.startswith("+"):
        return s2
    digits = re.sub(r"\D", "", s2)
    return "+34" + digits if len(digits) == 9 else s2

def normalize_case(text: str) -> str:
    def _cap(tok: str) -> str:
        return tok[0].upper() + tok[1:].lower() if tok else tok
    parts = re.split(r"(\s|-|')", (text or "").strip())
    return "".join(_cap(p) if i % 2 == 0 else p for i, p in enumerate(parts))

def split_name(full_name: str) -> Tuple[str, str]:
    full_name = re.sub(r"\s+", " ", full_name or "").strip()
    if not full_name:
        return "", ""
    parts = full_name.split(" ")
    return parts[0], " ".join(parts[1:]) if len(parts) > 1 else ""

def guess_name_lines(text: str, email_found: Optional[str], phone_found: Optional[str]) -> str:
    m_cancel = re.search(r"cita\s+para\s+([^\n\d]+?)\s+(?:consulta|valoración|estética)", text, flags=re.I)
    if m_cancel:
        return m_cancel.group(1).strip()
    m = re.search(r"¡\s*([^\n:]+?)\s*:\s*nueva\s+reserva", text, flags=re.I)
    if m:
        return m.group(1).strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    cleaned = []
    for ln in lines:
        lnl = ln.lower()
        if lnl.startswith("http") or "://" in lnl:
            continue
        if any(w in lnl for w in ["reserva","cancelada","booksy","consulta","valoración","tranquilidad",
                                  "ios","android","facebook","linkedin","youtube"]):
            continue
        if "@" in ln:
            continue
        ln = ln.lstrip("•").strip()
        cleaned.append(ln)
    if cleaned:
        return cleaned[0]
    return ""

def parse_patient(text: str) -> Dict[str, str]:
    m = EMAIL_RE.search(text or "")
    email = m.group(0).strip().lower() if m else ""
    m2 = PHONE_RE.search(text or "")
    phone = normalize_phone(m2.group(0)) if m2 else ""
    name_line = guess_name_lines(text, email, phone)
    nombre, apellidos = split_name(name_line)
    return {
        "nombre": normalize_case(nombre),
        "apellidos": normalize_case(apellidos),
        "telefono": phone,
        "email": email,
    }

# === Gmail ===
def list_booksy_messages(service, after=None, max_pages=50) -> List[Dict]:
    query = f"from:{BOOKSY_SENDER}"
    if after:
        query += f" after:{after}"
    msgs, page_token = [], None
    for _ in range(max_pages):
        resp = exec_retry(service.users().messages().list(userId="me", q=query, pageToken=page_token, maxResults=100))
        msgs += resp.get("messages", []) or []
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return msgs

def get_message_payload(service, msg_id: str) -> Tuple[str, str]:
    msg = exec_retry(service.users().messages().get(userId="me", id=msg_id, format="full"))
    internal_date = msg.get("internalDate")
    date_iso = datetime.fromtimestamp(int(internal_date)/1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z") if internal_date else ""
    payload = msg.get("payload", {})
    body_text = ""
    def _decode(b): return base64.urlsafe_b64decode(b.get("data","")).decode("utf-8", errors="ignore") if b.get("data") else ""
    if "parts" in payload:
        for p in payload["parts"]:
            mime = p.get("mimeType","")
            if mime == "text/plain":
                body_text += _decode(p.get("body",{}))
            elif mime == "text/html":
                body_text += re.sub("<[^>]+>", " ", _decode(p.get("body",{})))
    else:
        body_text += _decode(payload.get("body",{}))
    return date_iso, body_text

# === Proceso principal ===
def process():
    creds = get_creds()
    gmail, sheets, drive = gmail_service(creds), sheets_service(creds), drive_service(creds)
    ssid = get_or_create_spreadsheet_id(sheets, SHEET_TITLE, drive)
    ensure_sheets_and_headers(sheets, ssid)
    processed = read_processed_ids(sheets, ssid)
    emails = read_set_from_col(sheets, ssid, DATA_SHEET, "D")
    phones = read_set_from_col(sheets, ssid, DATA_SHEET, "C")
    ids = list_booksy_messages(gmail, max_pages=200)
    rows_d, rows_m = [], []
    for item in ids:
        mid = item.get("id")
        if not mid or mid in processed:
            continue
        date_iso, body = get_message_payload(gmail, mid)
        p = parse_patient(body)
        if not (p["nombre"] or p["apellidos"]) or not (p["telefono"] or p["email"]):
            rows_m.append([mid])
            continue
        if p["telefono"].replace(" ", "") in phones or p["email"] in emails:
            rows_m.append([mid])
            continue
        rows_d.append([p["nombre"], p["apellidos"], p["telefono"], p["email"], mid, date_iso])
        rows_m.append([mid])
        if p["telefono"]:
            phones.add(p["telefono"].replace(" ", ""))
        if p["email"]:
            emails.add(p["email"])
    append_rows(sheets, ssid, DATA_SHEET, rows_d)
    append_rows(sheets, ssid, META_SHEET, rows_m)
    sort_data_sheet_by_name(sheets, ssid, DATA_SHEET)
    export_patients_to_repo(sheets, ssid, DATA_SHEET)

if __name__ == "__main__":
    process()
