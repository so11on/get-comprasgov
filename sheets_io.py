# SUBSTITUA TODO O CONTEÚDO DE sheets_io.py POR ESTE CÓDIGO

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# --- Funções de Autenticação e Serviço ---

def _get_sheets_service(sa_info_json=None, sa_file_path=None) -> Any:
    """Cria e retorna um objeto de serviço autenticado para a API do Google Sheets."""
    creds = None
    if sa_info_json:
        creds = Credentials.from_service_account_info(sa_info_json, scopes=SCOPES)
    elif sa_file_path:
        creds = Credentials.from_service_account_file(sa_file_path, scopes=SCOPES)

    if not creds:
        raise RuntimeError("Credenciais do Google Sheets não configuradas. Defina GCP_SHEETS_SA_JSON ou GCP_SHEETS_SA_PATH.")

    return build('sheets', 'v4', credentials=creds).spreadsheets()

# --- Funções de Leitura e Processamento ---

def read_rows_ordered(spreadsheet_id: str, tab: str, sa_info_json=None, sa_file_path=None) -> list[dict[str, Any]]:
    """Lê todas as linhas de uma aba, mapeia colunas pelo cabeçalho e ordena por dataBusca."""
    svc = _get_sheets_service(sa_info_json=sa_info_json, sa_file_path=sa_file_path)
    
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
        id_compra_idx = header.index('idCompra')
        data_busca_idx = header.index('dataBusca')
        status_busca_idx = header.index('statusBusca')
    except ValueError as e:
        print(f"Erro: Coluna obrigatória não encontrada no cabeçalho: {e}. Cabeçalho encontrado: {header}")
        return []

    processed_rows = []
    for i, row in enumerate(data_rows):
        if len(row) <= max(id_compra_idx, data_busca_idx, status_busca_idx):
            continue

        processed_rows.append({
            'row_num': i + 2,
            'idCompra': row[id_compra_idx],
            'dataBusca': parse_iso(row[data_busca_idx]),
            'statusBusca': row[status_busca_idx]
        })

    processed_rows.sort(key=lambda x: (x['dataBusca'] is None, x['dataBusca']), reverse=True)
    
    return processed_rows

# --- Funções de Escrita ---

def batch_write_status(spreadsheet_id: str, tab: str, updates: list[tuple[int, str, str]], sa_info_json=None, sa_file_path=None, chunk_size: int = 300):
    """Escreve atualizações de status e data na planilha em lotes."""
    if not updates:
        return

    svc = _get_sheets_service(sa_info_json=sa_info_json, sa_file_path=sa_file_path)
    
    for i in range(0, len(updates), chunk_size):
        chunk = updates[i:i + chunk_size]
        data = []
        for row_num, data_busca, status_busca in chunk:
            data.append({
                'range': f"'{tab}'!G{row_num}:H{row_num}",
                'values': [[data_busca, status_busca]]
            })
        
        body = {
            'valueInputOption': 'RAW',
            'data': data
        }
        svc.values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

# --- Funções Utilitárias de Data ---

def parse_iso(s: str) -> datetime | None:
    """Converte string ISO 8601 para datetime, tratando o 'Z'."""
    if not s or not isinstance(s, str):
        return None
    try:
        s = s.replace('Z', '+00:00')
        if '.' in s:
            s = s.split('.')[0]
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None

def iso_utc_now() -> str:
    """Retorna o timestamp atual em UTC no formato ISO 8601 com 'Z'."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
