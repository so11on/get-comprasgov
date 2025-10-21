# -*- coding: utf-8 -*-
"""
PNCP Data Pipeline: Extração, Ordenação por dataBusca, Status e Gravação no Firestore

Atualizações implementadas:
1) Escrever data/hora da última consulta na coluna G (cabeçalho: "dataBusca").
2) Processar em ordem: primeiro linhas com "dataBusca" em branco; depois por "dataBusca" crescente (mais antiga -> mais nova).
3) Escrever na coluna H (cabeçalho: "statusBusca") um resumo da busca:
   - "X itens encontrados; X resultados encontrados; Sem modificações desde a última busca"
   - ou "X itens encontrados; X resultados encontrados; Dados atualizados no Banco"
   - Em caso de erro: "Erro: <mensagem resumida>"

Pré-requisitos (variáveis de ambiente):
- PLANILHA_ID, ABA_SHEETS
- UASG_ALVO (opcional)
- THROTTLE_SECONDS (opcional), HTTP_TIMEOUT_SECONDS (opcional), HTTP_USER_AGENT (opcional)
- DRY_RUN ("true" para não gravar no Firestore)
- Credenciais Google Sheets: GCP_SHEETS_SA_JSON (conteúdo) ou GCP_SHEETS_SA_PATH (arquivo)
- Credenciais Firestore: FIREBASE_SA_JSON (conteúdo) ou FIREBASE_SA_PATH (arquivo), FIRESTORE_PROJECT_ID

Observações:
- Coluna G = índice 7; Coluna H = índice 8.
- A planilha possui cabeçalho na linha 1.
- A ordenação é feita em memória; a planilha não é reordenada visualmente.
"""

import os
import io
import re
import csv
import json
import time
import typing as t
from datetime import datetime, timezone

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
import google.cloud.firestore_v1 as firestore

# ---------------------------
# Configurações
# ---------------------------

ID_RE = re.compile(r'^\d{17}$')  # ajuste se seu idCompra tiver outro formato
DEFAULT_TZ = "America/Sao_Paulo"

BASE = "https://pncp.gov.br/api"
EP_ITENS = f"{BASE}/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
EP_RES   = f"{BASE}/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
EP_CSV   = f"{BASE}/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"

COL_IDCOMPRA    = 1  # A
COL_DATA_BUSCA  = 7  # G
COL_STATUS      = 8  # H

# ---------------------------
# Utilitários
# ---------------------------

def _env_str(name: str, default: t.Optional[str] = None) -> t.Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and v != "") else default

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y")

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def format_ts_local(dt_utc: datetime, tz_name: str = DEFAULT_TZ) -> str:
    from zoneinfo import ZoneInfo
    dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")

def parse_data_busca(value: t.Any) -> t.Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    s = str(value).strip()
    if not s:
        return None

    from zoneinfo import ZoneInfo
    tz = ZoneInfo(DEFAULT_TZ)

    # ISO-like
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=tz)
        return d.astimezone(timezone.utc)
    except Exception:
        pass

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for fmt in fmts:
        try:
            d = datetime.strptime(s, fmt)
            d = d.replace(tzinfo=tz)
            return d.astimezone(timezone.utc)
        except Exception:
            continue
    return None

    headers = {}
    if ua:
        headers["User-Agent"] = ua
    return requests.get(url, params=params or {}, headers=headers, timeout=timeout)

# ---------------------------
# Google Sheets
# ---------------------------

def build_sheets_service():
    path = _env_str("GCP_SHEETS_SA_PATH")
    content = _env_str("GCP_SHEETS_SA_JSON")
    if not path and not content:
        raise RuntimeError("Credencial para Sheets ausente: defina GCP_SHEETS_SA_PATH ou GCP_SHEETS_SA_JSON.")
    if content:
        info = json.loads(content)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        creds = service_account.Credentials.from_service_account_file(
            path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def sheets_read_range(svc, spreadsheet_id: str, range_a1: str) -> list[list[t.Any]]:
    res = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    return res.get("values", [])

def sheets_write_range(svc, spreadsheet_id: str, range_a1: str, values: list[list[t.Any]]):
    body = {"values": values}
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_a1,
        valueInputOption="RAW",
        body=body
    ).execute()

def a1_range(sheet_name: str, start_row: int, start_col: int, end_row: int, end_col: int) -> str:
    def col_to_a(n: int) -> str:
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s
    return f"{sheet_name}!{col_to_a(start_col)}{start_row}:{col_to_a(end_col)}{end_row}"

# ---------------------------
# Firestore
# ---------------------------

def build_firestore_client():
    path = _env_str("FIREBASE_SA_PATH")
    content = _env_str("FIREBASE_SA_JSON")
    project_id = _env_str("FIRESTORE_PROJECT_ID")
    if not path and not content:
        raise RuntimeError("Credencial para Firestore ausente: defina FIREBASE_SA_PATH ou FIREBASE_SA_JSON.")
    if not project_id:
        raise RuntimeError("Defina FIRESTORE_PROJECT_ID.")
    if content:
        info = json.loads(content)
        creds = service_account.Credentials.from_service_account_info(info)
    else:
        creds = service_account.Credentials.from_service_account_file(path)
    return firestore.Client(project=project_id, credentials=creds)

# ---------------------------
# PNCP API
# ---------------------------

def consultar_itens(id_compra: str, timeout: int, ua: str | None) -> dict:
    r = http_get(EP_ITENS, params={"idCompra": id_compra}, timeout=timeout, ua=ua)
    r.raise_for_status()
    return r.json()

def consultar_resultados(id_compra: str, timeout: int, ua: str | None) -> dict:
    r = http_get(EP_RES, params={"idCompra": id_compra}, timeout=timeout, ua=ua)
    r.raise_for_status()
    return r.json()

def consultar_pesquisa_csv(codigo_material: str, timeout: int, ua: str | None) -> str:
    r = http_get(EP_CSV, params={"codigoMaterial": codigo_material}, timeout=timeout, ua=ua)
    r.raise_for_status()
    return r.text

# ---------------------------
# Normalização/Comparação
# ---------------------------

def normalizar_documento(d: dict) -> dict:
    x = dict(d or {})
    x.pop("updatedAt", None)
    return x

def documentos_diferem(a: dict, b: dict) -> bool:
    return normalizar_documento(a) != normalizar_documento(b)

# ---------------------------
# Persistência Firestore
# ---------------------------

def upsert_itens(db: firestore.Client, itens: list[dict], dry_run: bool):
    if not itens:
        return
    batch = db.batch()
    for it in itens:
        doc_id = f"{it.get('idCompra')}-{it.get('numeroItemPncp')}"
        ref = db.collection("ITENS").document(doc_id)
        if not dry_run:
            batch.set(ref, it, merge=True)
    if not dry_run:
        batch.commit()

def upsert_resultados(db: firestore.Client, resultados: list[dict], dry_run: bool) -> bool:
    houve_update = False
    if not resultados:
        return False
    batch = db.batch()
    for r in resultados:
        doc_id = f"{r.get('idCompra')}-{r.get('numeroItemPncp')}-{r.get('niFornecedor')}"
        ref = db.collection("RESULTADOS").document(doc_id)
        snap = ref.get()
        if snap.exists:
            atual = snap.to_dict()
            if documentos_diferem(atual, r):
                houve_update = True
                if not dry_run:
                    batch.set(ref, r, merge=True)
        else:
            houve_update = True
            if not dry_run:
                batch.set(ref, r, merge=True)
    if not dry_run:
        batch.commit()
    return houve_update

def upsert_pesquisa_precos(db: firestore.Client, docs: list[dict], dry_run: bool):
    if not docs:
        return
    batch = db.batch()
    for d in docs:
        doc_id = f"{d.get('idCompra')}-{d.get('numeroItemCompra')}"
        ref = db.collection("PESQUISA_PRECOS").document(doc_id)
        if not dry_run:
            batch.set(ref, d, merge=True)
    if not dry_run:
        batch.commit()

# ---------------------------
# CSV -> objetos (placeholder simples)
# ---------------------------

def parse_pesquisa_csv(csv_text: str, uasg_alvo: str | None) -> list[dict]:
    rows = []
    buf = io.StringIO(csv_text)
    reader = csv.DictReader(buf, delimiter=';')
    for r in reader:
        if uasg_alvo and str(r.get("codigoUasg") or "") != str(uasg_alvo):
            continue
        rows.append(r)
    return rows

# ---------------------------
# Sheets: leitura ordenada e escrita G/H
# ---------------------------

def ler_dados_planilha_ordenado(svc, spreadsheet_id: str, sheet_name: str) -> list[dict]:
    range_all = f"{sheet_name}!A1:Z"
    values = sheets_read_range(svc, spreadsheet_id, range_all)
    if not values:
        return []
    header = values[0]
    data = values[1:]

    registros = []
    for i, row in enumerate(data, start=2):
        while len(row) < max(COL_DATA_BUSCA, COL_STATUS):
            row.append("")
        raw_id = row[COL_IDCOMPRA - 1] if len(row) >= COL_IDCOMPRA else ""
        id_compra = str(raw_id).strip()
        if not id_compra or not ID_RE.match(id_compra):
            continue
        data_busca_raw = row[COL_DATA_BUSCA - 1]
        data_busca_dt = parse_data_busca(data_busca_raw)
        registros.append({
            "row_index": i,
            "idCompra": id_compra,
            "dataBusca_raw": data_busca_raw,
            "dataBusca_dt": data_busca_dt
        })

    # Ordenar: vazios primeiro, depois por data asc
    registros.sort(key=lambda it: (
        0 if it["dataBusca_dt"] is None else 1,
        it["dataBusca_dt"] or datetime.max.replace(tzinfo=timezone.utc)
    ))
    return registros

def escrever_stamp_e_status(svc, spreadsheet_id: str, sheet_name: str, row_index: int, ts_str: str, status: str):
    rng = a1_range(sheet_name, row_index, COL_DATA_BUSCA, row_index, COL_STATUS)  # G:H daquela linha
    sheets_write_range(svc, spreadsheet_id, rng, [[ts_str, status]])

# ---------------------------
# Status
# ---------------------------

def compor_status(qtd_itens: int, qtd_resultados: int, houve_update: bool, erro: str | None = None) -> str:
    if erro:
        msg = str(erro).strip()
        return f"Erro: {msg[:300]}"
    base = f"{qtd_itens} itens encontrados; {qtd_resultados} resultados encontrados; "
    return base + ("Dados atualizados no Banco" if houve_update else "Sem modificações desde a última busca")

# ---------------------------
# Processo principal
# ---------------------------

def processar():
    # Config
    spreadsheet_id = _env_str("PLANILHA_ID")
    sheet_name     = _env_str("ABA_SHEETS")
    uasg_alvo      = _env_str("UASG_ALVO")
    throttle_s     = _env_int("THROTTLE_SECONDS", 0)
    http_timeout   = _env_int("HTTP_TIMEOUT_SECONDS", 40)
    http_ua        = _env_str("HTTP_USER_AGENT", "pncp-bot/1.0")
    dry_run        = _env_bool("DRY_RUN", False)

    if not spreadsheet_id or not sheet_name:
        raise RuntimeError("Defina PLANILHA_ID e ABA_SHEETS.")

    # Serviços
    svc = build_sheets_service()
    db  = build_firestore_client()

    # Lê linhas ordenadas conforme regra
    filas = ler_dados_planilha_ordenado(svc, spreadsheet_id, sheet_name)

    for item in filas:
        row_idx   = item["row_index"]
        id_compra = item["idCompra"]

        ts_exec_utc = now_utc()
        ts_str      = format_ts_local(ts_exec_utc, DEFAULT_TZ)

        qtd_itens = 0
        qtd_res   = 0
        houve_upd = False
        status    = ""

        try:
            # 1) Itens
            js_itens = consultar_itens(id_compra, timeout=http_timeout, ua=http_ua)
            itens = js_itens if isinstance(js_itens, list) else js_itens.get("itens") or js_itens.get("content") or []
            qtd_itens = len(itens) if itens else 0

            # 2) Resultados
            js_res = consultar_resultados(id_compra, timeout=http_timeout, ua=http_ua)
            resultados = js_res if isinstance(js_res, list) else js_res.get("resultados") or js_res.get("content") or []
            qtd_res = len(resultados) if resultados else 0

            # Persistências
            upsert_itens(db, itens, dry_run=dry_run)
            houve_upd = upsert_resultados(db, resultados, dry_run=dry_run)

            # 3) Opcional: pesquisa de preços quando houve atualização de resultados
            if houve_upd:
                # Exemplo mínimo: se tiver codigoMaterial em itens, pega o primeiro
                cod_mats = []
                for it in itens or []:
                    cm = it.get("codigoMaterial") or it.get("codigoItem") or None
                    if cm:
                        cod_mats.append(str(cm))
                cod_mats = list(dict.fromkeys(cod_mats))  # únicos, mantendo ordem
                docs_pesquisa = []
                for cm in cod_mats[:3]:  # limite de segurança
                    csv_txt = consultar_pesquisa_csv(cm, timeout=http_timeout, ua=http_ua)
                    linhas = parse_pesquisa_csv(csv_txt, uasg_alvo)
                    # Anexe metadados mínimos
                    for ln in linhas:
                        ln["idCompra"] = id_compra
                        ln["numeroItemCompra"] = ln.get("numeroItemCompra") or ln.get("numeroItemPncp") or ""
                    docs_pesquisa.extend(linhas)
                    if throttle_s:
                        time.sleep(throttle_s)
                upsert_pesquisa_precos(db, docs_pesquisa, dry_run=dry_run)

            status = compor_status(qtd_itens, qtd_res, houve_upd, erro=None)

        except requests.HTTPError as he:
            status = compor_status(qtd_itens, qtd_res, houve_upd, erro=f"HTTP {he.response.status_code}")
        except Exception as e:
            status = compor_status(qtd_itens, qtd_res, houve_upd, erro=str(e))

        # Escreve G (dataBusca) e H (statusBusca) na linha correspondente
        try:
            escrever_stamp_e_status(svc, spreadsheet_id, sheet_name, row_idx, ts_str, status)
        finally:
            if throttle_s:
                time.sleep(throttle_s)

# Execução direta
if __name__ == "__main__":
    processar()