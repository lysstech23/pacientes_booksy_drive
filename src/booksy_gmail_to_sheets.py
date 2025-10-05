#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Booksy → Gmail → Google Sheets/Drive + export a repo outputs/.

Funciones clave:
- Lee Gmail (from:no-reply@booksy.com), parsea Nombre/Apellidos/Teléfono/Email con heurísticas robustas.
- Evita duplicados por Gmail Message-ID, email y teléfono.
- Escribe/actualiza Google Sheet 'directorio_pacientes' (en carpeta Drive 'Automatizaciones-no-tocar').
- Ordena por Nombre.
- Normaliza capitalización de nombres y apellidos (Title case estricto).
- Exporta a GitHub workspace: outputs/pacs_contacts.csv y outputs/pacs_contacts_index.json.
- Reutiliza hoja existente si se pasa GOOGLE_SHEETS_SPREADSHEET_ID; si no, busca por título y/o crea.

Requiere:
- Variables de entorno (en GitHub Actions → Secrets) para OAuth:
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN
- Opcional: GOOGLE_SHEETS_SPREADSHEET_ID para reusar la misma hoja siempre.

Ámbitos OAuth:
- https://www.googleapis.com/auth/gmail.readonly
- https://www.googleapis.com/auth/spreadsheets
- https://www.googleapis.com/auth/drive.file
"""

import os
import re
import csv
import json
import time
import base64
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

# Google APIs
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

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

# === Utilidades de OAuth por variables de entorno (sin usar archivos locales) ===
def _creds_from_env() -> Credentials:
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError(
            "Faltan GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN."
        )
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
    # refresco proactivo
    try:
        if not creds.valid:
            creds.refresh(Request())
    except Exception:
        # segundo intento tras breve espera
        time.sleep(1.0)
        creds.refresh(Request())
    return creds

# === Servicios Google ===
def gmail_service(creds: Credentials):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def sheets_service(creds: Credentials):
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def drive_service(creds: Credentials):
    return build("drive", "v3", credentials=creds, cache_discovery=False)

# === Drive helpers ===
def get_or_create_folder_id(drive, folder_name: str = "Automatizaciones-no-tocar") -> str:
    q = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    resp = drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=10).execute()
    items = resp.get("files", [])
    if items:
        return items[0]["id"]
    meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = drive.files().create(body=meta, fields="id").execute()
    return folder["id"]

def move_file_to_folder(drive, file_id: str, folder_id: str):
    # quitar de padres actuales y mover al deseado
    file_info = drive.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file_info.get("parents", []))
    drive.files().update(fileId=file_id, addParents=folder_id, removeParents=prev_parents, fields="id, parents").execute()

# === Sheets helpers ===
def get_or_create_spreadsheet_id(svc, title: str, drive=None) -> str:
    """
    Reutiliza un Spreadsheet por ID si GOOGLE_SHEETS_SPREADSHEET_ID está definido.
    Si no, busca por título en Drive; si no existe, crea uno y lo mueve a la carpeta 'Automatizaciones-no-tocar'.
    """
    # 1) Reusar por ID si está definido
    ssid = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if ssid:
        return ssid

    # 2) Intentar localizar por título (si tenemos Drive)
    if drive is not None:
        q = f"name = '{title}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
        resp = drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=5).execute()
        items = resp.get("files", [])
        if items:
            return items[0]["id"]

    # 3) Crear si no se encuentra
    body = {"properties": {"title": title}}
    created = svc.spreadsheets().create(body=body, fields="spreadsheetId").execute()
    ssid = created["spreadsheetId"]

    # mover a carpeta
    if drive is not None:
        folder_id = get_or_create_folder_id(drive, "Automatizaciones-no-tocar")
        move_file_to_folder(drive, ssid, folder_id)

    return ssid

def sheet_exists(svc, spreadsheet_id: str, title: str) -> bool:
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == title:
            return True
    return False

def get_sheet_id_by_title(svc, spreadsheet_id: str, title: str) -> Optional[int]:
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == title:
            return props.get("sheetId")
    return None

def ensure_sheets_and_headers(svc, spreadsheet_id: str):
    requests = []

    # crear pestaña DATA si falta
    if not sheet_exists(svc, spreadsheet_id, DATA_SHEET):
        requests.append({
            "addSheet": {
                "properties": {"title": DATA_SHEET}
            }
        })
    # crear pestaña META si falta
    if not sheet_exists(svc, spreadsheet_id, META_SHEET):
        requests.append({
            "addSheet": {
                "properties": {"title": META_SHEET}
            }
        })

    if requests:
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()

    # encabezados
    def set_headers(title: str, headers: List[str]):
        rng = f"{title}!A1:{chr(64 + len(headers))}1"
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()

    set_headers(DATA_SHEET, ["Nombre", "Apellidos", "Telefono", "Email", "Gmail_Message_ID", "Gmail_Date"])
    set_headers(META_SHEET, ["Gmail_Message_ID"])

def read_processed_ids(svc, spreadsheet_id: str) -> set:
    rng = f"{META_SHEET}!A2:A"
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    vals = resp.get("values", [])
    return {row[0] for row in vals if row}

def read_set_from_col(svc, spreadsheet_id: str, sheet_title: str, col_letter: str) -> set:
    rng = f"{sheet_title}!{col_letter}2:{col_letter}"
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    vals = resp.get("values", [])
    out = set()
    for row in vals:
        if not row:
            continue
        value = str(row[0]).strip()
        if not value:
            continue
        if col_letter.upper() == "C":  # Teléfono: normalizar quitando espacios
            value = value.replace(" ", "")
        out.add(value)
    return out

def append_rows(svc, spreadsheet_id: str, sheet_title: str, rows: List[List[str]]):
    if not rows:
        return
    rng = f"{sheet_title}!A:A"
    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

def sort_data_sheet_by_name(svc, spreadsheet_id: str, sheet_title: str):
    sheet_id = get_sheet_id_by_title(svc, spreadsheet_id, sheet_title)
    if sheet_id is None:
        return
    requests = [{
        "sortRange": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,  # no ordenar cabecera
                "startColumnIndex": 0,
            },
            "sortSpecs": [{"dimensionIndex": 0, "sortOrder": "ASCENDING"}]
        }
    }]
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()

def export_patients_to_repo(svc, spreadsheet_id: str,
                            sheet_title: str = DATA_SHEET,
                            out_dir: str = "outputs",
                            csv_name: str = "pacs_contacts.csv",
                            json_name: str = "pacs_contacts_index.json") -> None:
    os.makedirs(out_dir, exist_ok=True)
    rng = f"{sheet_title}!A1:F"
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=rng, valueRenderOption="UNFORMATTED_VALUE"
    ).execute()
    rows = resp.get("values", []) or []
    if not rows:
        return

    # CSV completo
    csv_path = os.path.join(out_dir, csv_name)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    # Índice JSON por teléfono normalizado
    header = rows[0]
    idx = {h.lower(): i for i, h in enumerate(header)}
    def col(name): return idx.get(name, -1)

    index = {}
    for r in rows[1:]:
        if not r:
            continue
        try:
            tel = str(r[col("telefono")]).replace(" ", "")
        except Exception:
            continue
        if not tel:
            continue
        entry = {
            "nombre": r[col("nombre")] if 0 <= col("nombre") < len(r) else "",
            "apellidos": r[col("apellidos")] if 0 <= col("apellidos") < len(r) else "",
            "email": r[col("email")] if 0 <= col("email") < len(r) else "",
            "gmail_id": r[col("gmail_message_id")] if 0 <= col("gmail_message_id") < len(r) else "",
            "fecha": r[col("gmail_date")] if 0 <= col("gmail_date") < len(r) else "",
        }
        index[tel] = entry

    json_path = os.path.join(out_dir, json_name)
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(index, jf, ensure_ascii=False, indent=2)

# === Parsing de mensajes ===
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?34[\s\-]?)?(?:\d[\s\-]?){9,13}")

def normalize_phone(s: str) -> str:
    # quita todo menos dígitos y '+', añade +34 si son 9 dígitos nacionales
    s2 = re.sub(r"[^\d+]", "", s or "")
    if s2.startswith("+"):
        return s2
    digits = re.sub(r"\D", "", s2)
    return "+34" + digits if len(digits) == 9 else s2

def normalize_case(text: str) -> str:
    """
    Title case conservador: 'mArIa eUgEnia MOReno' → 'Maria Eugenia Moreno'
    Mantiene apóstrofes y guiones correctamente.
    """
    def _cap(tok: str) -> str:
        if not tok:
            return tok
        return tok[0].upper() + tok[1:].lower()
    parts = re.split(r"(\s|-|')", (text or "").strip())
    return "".join(_cap(p) if i % 2 == 0 else p for i, p in enumerate(parts))

def split_name(full_name: str) -> Tuple[str, str]:
    full_name = re.sub(r"\s+", " ", full_name or "").strip()
    if not full_name:
        return "", ""
    parts = full_name.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def guess_name_lines(text: str, email_found: Optional[str], phone_found: Optional[str]) -> str:
    """
    Heurísticas para nombres en correos Booksy:
    - Patrones de "¡Nombre: nueva reserva"
    - Patrones de cancelación "cita para <nombre> consulta/valoración"
    - Candidatos cercanos a teléfono/email
    - Descarta URLs, líneas con '@', palabras clave irrelevantes, branding
    - Acepta líneas con viñeta '•' borrando la viñeta
    """
    # Cancelación / cita para
    m_cancel = re.search(
        r"cita\s+para\s+([^\n\d]+?)\s+(?:consulta|valoración|estética)", text, flags=re.I
    )
    if m_cancel:
        return m_cancel.group(1).strip()

    # "¡Nombre: nueva reserva"
    m = re.search(r"¡\s*([^\n:]+?)\s*:\s*nueva\s+reserva", text, flags=re.I)
    if m:
        return m.group(1).strip()

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    cleaned = []
    for ln in lines:
        lnl = ln.lower()
        # descartar URLs o líneas claramente no-nombre
        if lnl.startswith("http") or "://" in lnl:
            continue
        if "reserva" in lnl or "cancelada" in lnl or "booksy" in lnl:
            continue
        if "consulta" in lnl or "valoración" in lnl or "tranquilidad" in lnl:
            continue
        if "ios" in lnl or "android" in lnl or "facebook" in lnl or "linkedin" in lnl or "youtube" in lnl:
            continue
        if "@" in ln:
            continue  # evita confundir emails raros con nombres
        # limpiar viñetas
        ln = ln.lstrip("•").strip()
        cleaned.append(ln)

    # priorizar candidatos por capitalización y cercanía a teléfono/email
    def _score(line: str) -> int:
        toks = [t for t in line.split() if t]
        score = 0
        for t in toks:
            if len(t) > 1 and t[0].isalpha():
                score += 1
        return score

    candidates = sorted(cleaned[:30], key=_score, reverse=True)
    if candidates:
        return candidates[0]

    return ""

def parse_patient(text: str) -> Dict[str, str]:
    # Email
    email = None
    m = EMAIL_RE.search(text or "")
    if m:
        email = m.group(0).strip().lower()

    # Teléfono
    phone = None
    m2 = PHONE_RE.search(text or "")
    if m2:
        phone = normalize_phone(m2.group(0))

    # Nombre (línea heurística)
    name_line = guess_name_lines(text or "", email, phone)
    nombre, apellidos = split_name(name_line)

    # Normalización de nombres y apellidos (Title case conservador)
    nombre = normalize_case(nombre)
    apellidos = normalize_case(apellidos)

    return {
        "nombre": nombre,
        "apellidos": apellidos,
        "telefono": phone or "",
        "email": email or "",
    }

# === Gmail fetch ===
def list_booksy_messages(service, after: Optional[str] = None, max_pages: int = 50) -> List[Dict]:
    """
    Lista mensajes de Gmail desde BOOKSY. `after` en formato YYYY/MM/DD para histórico si se desea.
    """
    query = f"from:{BOOKSY_SENDER}"
    if after:
        query += f" after:{after}"
    msgs = []
    page_token = None
    for _ in range(max_pages):
        resp = service.users().messages().list(
            userId="me", q=query, pageToken=page_token, maxResults=100
        ).execute()
        ids = resp.get("messages", []) or []
        msgs.extend(ids)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return msgs

def get_message_payload(service, msg_id: str) -> Tuple[str, str]:
    """
    Devuelve (fecha_iso_utc, cuerpo_texto) para un mensaje dado.
    """
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    # fecha
    internal_date = msg.get("internalDate")
    if internal_date:
        dt = datetime.fromtimestamp(int(internal_date)/1000.0, tz=timezone.utc)
        date_iso = dt.isoformat().replace("+00:00", "Z")
    else:
        date_iso = ""

    # cuerpo
    payload = msg.get("payload", {})
    body_text = ""

    def _decode(part_body):
        data = part_body.get("data")
        if not data:
            return ""
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")

    if "parts" in payload:
        for p in payload["parts"]:
            mime = p.get("mimeType", "")
            if mime == "text/plain":
                body_text += _decode(p.get("body", {}))
            elif mime == "text/html":
                # como fallback, incluir texto llano de HTML también
                body_text += re.sub("<[^>]+>", " ", _decode(p.get("body", {})))
    else:
        body_text += _decode(payload.get("body", {}))

    return date_iso, body_text

# === Proceso principal ===
def process():
    creds = get_creds()
    gmail = gmail_service(creds)
    sheets = sheets_service(creds)
    drive = drive_service(creds)

    ssid = get_or_create_spreadsheet_id(sheets, SHEET_TITLE, drive)
    ensure_sheets_and_headers(sheets, ssid)

    processed_ids = read_processed_ids(sheets, ssid)
    existing_emails = read_set_from_col(sheets, ssid, DATA_SHEET, "D")
    existing_phones = read_set_from_col(sheets, ssid, DATA_SHEET, "C")

    # historial completo la primera vez; después, solo nuevos
    after = None  # si quisieras acotar: after="2022/01/01"
    ids = list_booksy_messages(gmail, after=after, max_pages=200)

    rows_data: List[List[str]] = []
    rows_meta: List[List[str]] = []

    for item in ids:
        mid = item.get("id")
        if not mid or mid in processed_ids:
            continue

        date_iso, body = get_message_payload(gmail, mid)
        parsed = parse_patient(body)
        nombre = parsed["nombre"]
        apellidos = parsed["apellidos"]
        telefono = (parsed["telefono"] or "").replace(" ", "")
        email = parsed["email"]

        # validez mínima
        if not (nombre or apellidos) or not (telefono or email):
            # marcar como procesado para no reintentar eternamente
            rows_meta.append([mid])
            continue

        # deduplicación por email/teléfono
        if telefono and telefono in existing_phones:
            rows_meta.append([mid])
            continue
        if email and email in existing_emails:
            rows_meta.append([mid])
            continue

        # almacenar nueva fila
        rows_data.append([nombre, apellidos, telefono, email, mid, date_iso])
        rows_meta.append([mid])

        # actualizar sets en caliente para evitar duplicados en el mismo batch
        if telefono:
            existing_phones.add(telefono)
        if email:
            existing_emails.add(email)

    # escribir en Sheets
    append_rows(sheets, ssid, DATA_SHEET, rows_data)
    append_rows(sheets, ssid, META_SHEET, rows_meta)

    # ordenar por Nombre (columna A)
    sort_data_sheet_by_name(sheets, ssid, DATA_SHEET)

    # exportar a repo
    export_patients_to_repo(sheets, ssid, DATA_SHEET)

if __name__ == "__main__":
    process()
