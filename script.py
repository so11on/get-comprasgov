# -*- coding: utf-8 -*-
"""
PNCP Data Pipeline: leitura do Google Sheets (Buscas), consultas PNCP, gravação no Firestore,
ordenação por dataBusca, carimbo de data/hora e status na planilha, com escrita em lote.

Atende:
1) Escrever em "dataBusca" a data/hora da última consulta.
2) Processar em ordem: primeiro dataBusca vazia; depois dataBusca crescente.
3) Escrever em "statusBusca" o resumo da busca.
4) Corrige erro: name 'http_get' is not defined (função reintroduzida).
5) Evita 429 no Sheets: usa values.batchUpdate com retry/backoff.

Defaults exigidos:
- PLANILHA_ID = 1JZfkhwmgnSSpaRo9Bnr0mP7VPhEWO0XkDZAbm4fAY9Q
- ABA_SHEETS  = Buscas
- Cabeçalhos: idCompra, dataBusca, statusBusca (case-insensitive)
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
from googleapiclient.errors import HttpError
import google.cloud.firestore_v1 as firestore

# ---------------------------
# Config
# ---------------------------

DEFAULT_TZ = "America/Sao_Paulo"

BASE = "https://pncp.gov.br/api"
EP_ITENS = f"{BASE}/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
EP_RES   = f"{BASE}/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
EP_CSV   = f"{BASE}/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"

HDR_IDCOMPRA   = "idCompra"
HDR_DATABUSCA  = "dataBusca"
HDR_STATUS     = "statusBusca"

# ---------------------------
# Utils
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

# Reintroduzido: http_get ausente em versões anteriores
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

def batch_update_values_with_retry(svc, spreadsheet_id: str, data_payload: list[dict], value_input_option: str = "RAW", max_retries: int = 5):
    if not data_payload:
        return
    body = {
        "valueInputOption": value_input_option,
        "data": data_payload
    }
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            return
        except HttpError as e:
            status = getattr(e, "status_code", None)
            if status is None and hasattr(e, "resp") and hasattr(e.resp, "status"):
                status = e.resp.status
            if status in (429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep(delay)
                delay = min(delay * 2, 16)
                continue
            raise

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
# Firestore upserts
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
# CSV -> objetos
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
# Sheets: mapeamento por cabeçalho, leitura ordenada, escrita em lote
# ---------------------------

def _mapear_colunas_por_nome(header: list[t.Any]) -> dict:
    if not header:
        raise RuntimeError("Cabeçalho da planilha ausente.")
    name_to_idx = {}
    for idx, nome in enumerate(header, start=1):
        key = str(nome).strip().lower() if nome is not None else ""
        if key:
            name_to_idx[key] = idx
    def achar(nome_alvo: str) -> int:
        k = nome_alvo.strip().lower()
        if k not in name_to_idx:
            raise RuntimeError(f"Coluna '{nome_alvo}' não encontrada no cabeçalho.")
        return name_to_idx[k]
    return {
        "id_col": achar(HDR_IDCOMPRA),
        "data_col": achar(HDR_DATABUSCA),
        "status_col": achar(HDR_STATUS),
    }

def ler_dados_planilha_ordenado(svc, spreadsheet_id: str, sheet_name: str, id_regex: t.Optional[re.Pattern]):
    values = sheets_read_range(svc, spreadsheet_id, f"{sheet_name}!A1:ZZ")
    if not values:
        print("[Sheets] Leitura vazia da planilha.")
        return [], None
    header = values[0]
    cols = _mapear_colunas_por_nome(header)
    id_col, data_col, status_col = cols["id_col"], cols["data_col"], cols["status_col"]
    data = values[1:]
    registros = []
    for i, row in enumerate(data, start=2):
        faltam = max(id_col, data_col, status_col) - len(row)
        if faltam > 0:
            row = row + [""] * faltam
        raw_id = row[id_col - 1]
        id_compra = str(raw_id).strip() if raw_id is not None else ""
        if not id_compra:
            continue
        if id_regex and not id_regex.match(id_compra):
            continue
        data_busca_raw = row[data_col - 1] if len(row) >= data_col else ""
        data_busca_dt = parse_data_busca(data_busca_raw)
        registros.append({
            "row_index": i,
            "idCompra": id_compra,
            "dataBusca_raw": data_busca_raw,
            "dataBusca_dt": data_busca_dt
        })
    registros.sort(key=lambda it: (
        0 if it["dataBusca_dt"] is None else 1,
        it["dataBusca_dt"] or datetime.max.replace(tzinfo=timezone.utc)
    ))
    print(f"[Sheets] Colunas: id={id_col}, dataBusca={data_col}, statusBusca={status_col}. Linhas elegíveis={len(registros)}")
    return registros, cols

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
# Principal
# ---------------------------

def processar():
    spreadsheet_id = _env_str("PLANILHA_ID", "1JZfkhwmgnSSpaRo9Bnr0mP7VPhEWO0XkDZAbm4fAY9Q")
    sheet_name     = _env_str("ABA_SHEETS", "Buscas")
    uasg_alvo      = _env_str("UASG_ALVO")
    throttle_s     = _env_int("THROTTLE_SECONDS", 0)
    http_timeout   = _env_int("HTTP_TIMEOUT_SECONDS", 40)
    http_ua        = _env_str("HTTP_USER_AGENT", "pncp-bot/1.0")
    dry_run        = _env_bool("DRY_RUN", False)
    id_regex_env   = _env_str("IDCOMPRA_REGEX")
    id_regex       = re.compile(id_regex_env) if id_regex_env else None

    print("[Init] Iniciando processamento PNCP.")
    print(f"[Env] PLANILHA_ID={spreadsheet_id!r} ABA_SHEETS={sheet_name!r} DRY_RUN={dry_run}")

    svc = build_sheets_service()
    print("[Sheets] Serviço Google Sheets inicializado.")
    db  = build_firestore_client()
    print("[Firestore] Cliente Firestore inicializado.")

    filas, cols = ler_dados_planilha_ordenado(svc, spreadsheet_id, sheet_name, id_regex)
    if not filas:
        print("[Processo] Nenhuma linha elegível encontrada. Encerrando.")
        return

    updates_payload: list[dict] = []  # ranges G:H para batchUpdate ao final

    for n, item in enumerate(filas, start=1):
        row_idx   = item["row_index"]
        id_compra = item["idCompra"]
        ts_exec_utc = now_utc()
        ts_str      = format_ts_local(ts_exec_utc, DEFAULT_TZ)

        qtd_itens = 0
        qtd_res   = 0
        houve_upd = False
        status    = ""

        print(f"[Exec] ({n}/{len(filas)}) idCompra={id_compra} linha={row_idx}")

        try:
            js_itens = consultar_itens(id_compra, timeout=http_timeout, ua=http_ua)
            itens = js_itens if isinstance(js_itens, list) else js_itens.get("itens") or js_itens.get("content") or []
            qtd_itens = len(itens) if itens else 0
            print(f"[API] Itens={qtd_itens}")

            js_res = consultar_resultados(id_compra, timeout=http_timeout, ua=http_ua)
            resultados = js_res if isinstance(js_res, list) else js_res.get("resultados") or js_res.get("content") or []
            qtd_res = len(resultados) if resultados else 0
            print(f"[API] Resultados={qtd_res}")

            upsert_itens(db, itens, dry_run=dry_run)
            houve_upd = upsert_resultados(db, resultados, dry_run=dry_run)
            print(f"[DB] RESULTADOS atualizados={houve_upd}")

            if houve_upd:
                cod_mats = []
                for it in itens or []:
                    cm = it.get("codigoMaterial") or it.get("codigoItem")
                    if cm:
                        cod_mats.append(str(cm))
                cod_mats = list(dict.fromkeys(cod_mats))
                docs_pesquisa = []
                for cm in cod_mats[:3]:
                    csv_txt = consultar_pesquisa_csv(cm, timeout=http_timeout, ua=http_ua)
                    linhas = parse_pesquisa_csv(csv_txt, uasg_alvo)
                    for ln in linhas:
                        ln["idCompra"] = id_compra
                        ln["numeroItemCompra"] = ln.get("numeroItemCompra") or ln.get("numeroItemPncp") or ""
                    docs_pesquisa.extend(linhas)
                    if throttle_s:
                        time.sleep(throttle_s)
                upsert_pesquisa_precos(db, docs_pesquisa, dry_run=dry_run)
                print(f"[DB] Pesquisa de preços gravada={len(docs_pesquisa)}")

            status = compor_status(qtd_itens, qtd_res, houve_upd, erro=None)

        except requests.HTTPError as he:
            code = he.response.status_code if he.response is not None else "HTTP"
            status = compor_status(qtd_itens, qtd_res, houve_upd, erro=f"HTTP {code}")
            print(f"[Erro] HTTP {code} em idCompra={id_compra}")
        except Exception as e:
            status = compor_status(qtd_itens, qtd_res, houve_upd, erro=str(e))
            print(f"[Erro] Exceção em idCompra={id_compra}: {e}")

        # Acumula G/H (dataBusca/statusBusca) para batchUpdate
        data_col   = cols["data_col"]
        status_col = cols["status_col"]
        rng = a1_range(sheet_name, row_idx, data_col, row_idx, status_col)
        updates_payload.append({
            "range": rng,
            "values": [[ts_str, status]]
        })

        if throttle_s:
            time.sleep(throttle_s)

    # Escrita em lote no Sheets
    print(f"[Sheets] Enviando batchUpdate: {len(updates_payload)} linhas...")
    batch_update_values_with_retry(svc, spreadsheet_id, updates_payload, value_input_option="RAW", max_retries=5)
    print("[Fim] Processamento concluído.")

if __name__ == "__main__":
    processar()