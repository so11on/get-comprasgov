from __future__ import annotations
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _get_sheets_service(sa_info_json=None, sa_file_path=None):
    if sa_info_json:
        creds = Credentials.from_service_account_info(sa_info_json, scopes=SCOPES)
    elif sa_file_path:
        creds = Credentials.from_service_account_file(sa_file_path, scopes=SCOPES)
    else:
        raise RuntimeError("Credenciais do Google Sheets não configuradas")
    return build("sheets", "v4", credentials=creds).spreadsheets()

def read_rows_ordered(spreadsheet_id: str, tab: str,
                      sa_info_json=None, sa_file_path=None) -> List[Dict[str, Any]]:
    svc = _get_sheets_service(sa_info_json, sa_file_path)
    rng = f"{tab}!A:H"
    resp = svc.values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])

    if not values:
        return []

    header = values[0]
    rows = values[1:]
    max_len = max((len(r) for r in rows), default=0)
    rows = [r + ["" * 0] * 0 for r in rows]  # no-op para manter estrutura

    # normaliza largura das linhas para o tamanho do cabeçalho (H colunas)
    expected_len = max(len(header), 8)
    rows = [r + [""] * (expected_len - len(r)) if len(r) < expected_len else r[:expected_len] for r in rows]

        return header.index(name) if name in header else fallback

    i_id = col_idx("idCompra", 0)          # A
    i_data = col_idx("dataBusca", 6)       # G
    i_status = col_idx("statusBusca", 7)   # H

    def parse_iso(s: str):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows, start=2):  # dados começam na linha 2
        item = {
            "row_number": i,
            "idCompra": (r[i_id] if i_id is not None and i_id < len(r) else "").strip(),
            "dataBusca": (r[i_data] if i_data is not None and i_data < len(r) else "").strip(),
            "statusBusca": (r[i_status] if i_status is not None and i_status < len(r) else "").strip(),
        }
        out.append(item)

    def key(d: Dict[str, Any]):
        dt = parse_iso(d["dataBusca"])
        # primeiro vazios, depois datas mais antigas primeiro
        return (0 if not d["dataBusca"] else 1, dt or datetime.max.replace(tzinfo=timezone.utc))

    out.sort(key=key)
    return out

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def batch_write_status(spreadsheet_id: str, tab: str, updates: List[Tuple[int, str, str]],
                       sa_info_json=None, sa_file_path=None, chunk_size: int = 300):
    svc = _get_sheets_service(sa_info_json, sa_file_path)
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i:i+chunk_size]
        data = []
        for row_number, dataBusca, statusBusca in chunk:
            rng = f"{tab}!G{row_number}:H{row_number}"
            data.append({"range": rng, "values": [[dataBusca, statusBusca]]})
        body = {"valueInputOption": "RAW", "data": data}
        svc.values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()