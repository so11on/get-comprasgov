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

# SUBSTITUA A FUNÇÃO 'read_rows_ordered' INTEIRA EM sheets_io.py

def read_rows_ordered(spreadsheet_id: str, tab: str, sa_info_json=None, sa_file_path=None) -> list[dict[str, any]]:
    """Lê todas as linhas de uma aba, mapeia colunas pelo cabeçalho e ordena por dataBusca."""
    svc = _get_sheets_service(sa_info_json=sa_info_json, sa_file_path=sa_file_path)
    
    # Define o range para ler a planilha inteira
    rng = f"'{tab}'!A:H"
    
    try:
        resp = svc.values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
        values = resp.get('values', [])
    except Exception as e:
        print(f"Erro ao acessar a planilha: {e}")
        return []

    if not values:
        print("A planilha está vazia.")
        return []

    header = [h.strip() for h in values[0]]
    data_rows = values[1:]

    try:
        # Mapeia os índices das colunas necessárias a partir do cabeçalho
        id_compra_idx = header.index('idCompra')
        data_busca_idx = header.index('dataBusca')
        status_busca_idx = header.index('statusBusca')
    except ValueError as e:
        print(f"Erro: Coluna obrigatória não encontrada no cabeçalho: {e}. Cabeçalho encontrado: {header}")
        return []

    processed_rows = []
    for i, row in enumerate(data_rows):
        # Garante que a linha tenha o número mínimo de colunas
        if len(row) <= max(id_compra_idx, data_busca_idx, status_busca_idx):
            continue

        processed_rows.append({
            'row_num': i + 2,  # +1 para o cabeçalho, +1 para o índice baseado em 0
            'idCompra': row[id_compra_idx],
            'dataBusca': parse_iso(row[data_busca_idx]),
            'statusBusca': row[status_busca_idx]
        })

    # Ordena as linhas: nulas primeiro, depois por data
    processed_rows.sort(key=lambda x: (x['dataBusca'] is None, x['dataBusca']), reverse=True)
    
    return processed_rows

    def parse_iso(s: str) -> datetime | None:
    """Converte string ISO 8601 para datetime, tratando o 'Z'."""
    if not s or not isinstance(s, str):
        return None
    try:
        # Substitui 'Z' para compatibilidade e remove microssegundos
        s = s.replace('Z', '+00:00')
        if '.' in s:
            s = s.split('.')[0]
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None

    def sort_key(d: Dict[str, Any]):
        dt = parse_iso(d["dataBusca"])
        # 1) vazios primeiro; 2) datas válidas crescentes
        return (0 if not d["dataBusca"] else 1, dt or datetime.max.replace(tzinfo=timezone.utc))

    out.sort(key=sort_key)
    return out

def iso_utc_now() -> str:
    """Retorna o timestamp atual em UTC no formato ISO 8601 com 'Z'."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')

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