# -*- coding: utf-8 -*-
"""
Pipeline PNCP -> Firestore
- Lê idCompra do Google Sheets (aba 'Buscas', coluna 'idCompra')
- Busca 2.1 (Itens) e 3.1 (Resultados)
- Busca Pesquisa de Preços (CSV) apenas se faltar no cache (coleção PESQUISA_PRECOS),
  filtrando por idCompra, numeroItemCompra e codigoUasg == UASG_ALVO
- Salva nas coleções: ITENS, RESULTADOS, PESQUISA_PRECOS
Obs.: RESULTADOS NÃO recebe campo 'marca' (apenas em PESQUISA_PRECOS)
"""

import os
import io
import re
import json
import time
import logging
from typing import Any, Dict, List, Optional, Set, Iterable, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv as pycsv

import gspread
from google.oauth2.service_account import Credentials
from google.cloud import firestore

# === Correção robusta de encoding (CSV) ===
from charset_normalizer import from_bytes
from ftfy import fix_text

# ================= CONFIG =================
# Use ENV para substituir defaults
PLANILHA_ID_DEFAULT = "1JZfkhwmgnSSpaRo9Bnr0mP7VPhEWO0XkDZAbm4fAY9Q"
ABA_SHEETS_DEFAULT = "Buscas"

PLANILHA_ID = os.getenv("PLANILHA_ID", PLANILHA_ID_DEFAULT)
ABA_SHEETS = os.getenv("ABA_SHEETS", ABA_SHEETS_DEFAULT)

BASE = "https://dadosabertos.compras.gov.br"
EP_ITENS = f"{BASE}/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
EP_RES   = f"{BASE}/modulo-contratacoes/3.1_consultarResultadoItensContratacoes_PNCP_14133_Id"
EP_CSV   = f"{BASE}/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"

COL_ITENS = "ITENS"
COL_RES   = "RESULTADOS"
COL_PRECO = "PESQUISA_PRECOS"

# UASG alvo para filtrar linhas do CSV (coluna 'codigoUasg')
UASG_ALVO = os.getenv("UASG_ALVO", "986001")

# Pausa anti-bloqueio (default 5s por request) — ajuste via env THROTTLE_SECONDS
THROTTLE_SECONDS = int(os.getenv("THROTTLE_SECONDS", "5"))

# Timeout padrão de requests
DEFAULT_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))

# User-Agent identificável
UA = os.getenv("HTTP_USER_AGENT", "pncp-sync/1.0 (+contato: seu-email@dominio)")

# DRY_RUN: não grava no Firestore quando true
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Limites de segurança
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))
BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", "450"))
GET_ALL_CHUNK = int(os.getenv("GET_ALL_CHUNK", "300"))

# ================= CAMPOS CANÔNICOS =================
# ITENS: exatamente os campos especificados
ITENS_KEEP = {
    "idCompra",
    "idCompraItem",
    "idContratacaoPNCP",
    "unidadeOrgaoCodigoUnidade",
    "orgaoEntidadeCnpj",
    "numeroItemPncp",
    "numeroItemCompra",
    "numeroGrupo",
    "descricaoResumida",
    "materialOuServicoNome",
    "codigoClasse",
    "codigoGrupo",
    "codItemCatalogo",
    "descricaodetalhada",
    "unidadeMedida",
    "situacaoCompraItemNome",
    "tipoBeneficioNome",
    "quantidade",
    "valorUnitarioEstimado",
    "valorTotal",
    "temResultado",
    "codFornecedor",
    "nomeFornecedor",
    "quantidadeResultado",
    "valorUnitarioResultado",
    "valorTotalResultado",
    "dataResultado",
    "numeroControlePNCPCompra",
}

# RESULTADOS: exatamente os campos especificados
RESULTADOS_KEEP = {
    "idCompraItem",
    "idCompra",
    "idContratacaoPNCP",
    "unidadeOrgaoCodigoUnidade",
    "numeroItemPncp",
    "sequencialResultado",
    "niFornecedor",
    "tipoPessoa",
    "nomeRazaoSocialFornecedor",
    "ordemClassificacaoSrp",
    "quantidadeHomologada",
    "valorUnitarioHomologado",
    "valorTotalHomologado",
    "percentualDesconto",
    "situacaoCompraItemResultadoNome",
    "porteFornecedorNome",
    "naturezaJuridicaNome",
    "dataInclusaoPncp",
    "dataAtualizacaoPncp",
    "dataCancelamentoPncp",
    "dataResultadoPncp",
    "numeroControlePNCPCompra",
    "aplicacaoBeneficioMeepp",
    "aplicacaoCriterioDesempate",
    "amparoLegalCriterioDesempateNome",
    "moedaEstrangeiraId",
    "dataCotacaoMoedaEstrangeira",
    "valorNominalMoedaEstrangeira",
    "paisOrigemProdutoServicoId",
}

# PESQUISA_PRECOS: mapear CSV -> chaves canônicas (armazenar só essas)
PRECO_FIELD_CANDIDATES: Dict[str, List[str]] = {
    "idCompra": ["idCompra", "id_compra"],
    "idItemCompra": ["idItemCompra", "id_item_compra", "idItem", "idItemPncp"],
    "modalidade": ["modalidade", "Modalidade"],
    "numeroItemCompra": ["numeroItemCompra", "numero_item_compra", "numeroItemPncp", "nItem"],
    "descricaoItem": ["descricaoItem", "descricao_item", "descricao", "descricaoResumida"],
    "codigoItemCatalogo": ["codigoItemCatalogo", "codItemCatalogo", "codigo_item_catalogo"],
    "siglaUnidadeMedida": ["siglaUnidadeMedida", "sigla_unidade_medida", "siglaUnidade", "siglaUnidMedida"],
    "nomeUnidadeFornecimento": ["nomeUnidadeFornecimento", "nome_unidade_fornecimento"],
    "siglaUnidadeFornecimento": ["siglaUnidadeFornecimento", "sigla_unidade_fornecimento"],
    "quantidade": ["quantidade", "qtde", "qtd"],
    "precoUnitario": ["precoUnitario", "preco_unitario", "valorUnitario"],
    "percentualMaiorDesconto": ["percentualMaiorDesconto", "percMaiorDesconto"],
    "niFornecedor": ["niFornecedor", "cnpjCpfFornecedor", "cnpjFornecedor"],
    "nomeFornecedor": ["nomeFornecedor", "razaoSocialFornecedor", "fornecedor"],
    "marca": ["marca", "Marca"],
    "codigoUasg": ["codigoUasg", "codigo_uasg", "uasg", "codigoUASG"],
    "nomeUasg": ["nomeUasg", "NomeUasg"],
    "municipio": ["municipio", "Município", "cidade"],
    "dataCompra": ["dataCompra", "data_compra"],
    "dataHoraAtualizacaoCompra": ["dataHoraAtualizacaoCompra", "dtHrAtualizacaoCompra"],
    "dataHoraAtualizacaoItem": ["dataHoraAtualizacaoItem", "dtHrAtualizacaoItem"],
    "dataResultado": ["dataResultado", "data_resultado"],
    "dataHoraAtualizacaoUasg": ["dataHoraAtualizacaoUasg", "dtHrAtualizacaoUasg"],
    "codigoClasse": ["codigoClasse", "codClasse"],
}

# ================= LOG =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("pncp-sync")

# ============== Credenciais (Sheets x Firestore) ==============
def build_sheets_credentials() -> Credentials:
    """
    Lê credenciais do Sheets de:
    - GCP_SHEETS_SA_JSON (conteúdo JSON) OU
    - GCP_SHEETS_SA_PATH (caminho do arquivo JSON escrito pelo workflow)
    Escopos readonly.
    """
    sa_json = os.getenv("GCP_SHEETS_SA_JSON")
    sa_path = os.getenv("GCP_SHEETS_SA_PATH")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    if sa_json:
        info = json.loads(sa_json)
        return Credentials.from_service_account_info(info, scopes=scopes)
    if sa_path:
        return Credentials.from_service_account_file(sa_path, scopes=scopes)
    raise RuntimeError("Defina GCP_SHEETS_SA_JSON ou GCP_SHEETS_SA_PATH para Sheets.")

def build_firestore_credentials() -> Credentials:
    """
    Lê credenciais do Firestore de:
    - FIREBASE_SA_JSON (conteúdo JSON) OU
    - FIREBASE_SA_PATH (caminho do arquivo JSON escrito pelo workflow)
    Sem escopos.
    """
    sa_json = os.getenv("FIREBASE_SA_JSON")
    sa_path = os.getenv("FIREBASE_SA_PATH")
    if sa_json:
        info = json.loads(sa_json)
        return Credentials.from_service_account_info(info)
    if sa_path:
        return Credentials.from_service_account_file(sa_path)
    raise RuntimeError("Defina FIREBASE_SA_JSON ou FIREBASE_SA_PATH para Firestore.")

def make_firestore_client(creds: Credentials) -> firestore.Client:
    project = os.getenv("FIRESTORE_PROJECT_ID", None)
    if not project:
        raise RuntimeError("Defina FIRESTORE_PROJECT_ID (ex.: get-comprasgov).")
    return firestore.Client(project=project, credentials=creds)

def make_sheets_client(creds: Credentials) -> gspread.Client:
    return gspread.authorize(creds)

# ============== HTTP com retry + throttle ==============
_last_call = 0.0
def throttle():
    global _last_call
    now = time.time()
    delta = now - _last_call
    if delta < THROTTLE_SECONDS:
        time.sleep(max(0, THROTTLE_SECONDS - delta))
    _last_call = time.time()

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def get_json(session: requests.Session, url: str, params: Dict[str, Any]) -> Any:
    throttle()
    r = session.get(
        url,
        params=params,
        headers={"Accept": "application/json", "User-Agent": UA},
        timeout=DEFAULT_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def get_bytes(session: requests.Session, url: str, params: Dict[str, Any]) -> bytes:
    throttle()
    r = session.get(
        url,
        params=params,
        headers={"Accept": "text/csv, */*", "User-Agent": UA},
        timeout=DEFAULT_TIMEOUT,
    )
    r.raise_for_status()
    return r.content

# ============== Helpers ==============
def normalize_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s or None

def only_fields(d: Dict[str, Any], keep: set) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if k in keep}

def pick_num_item_pncp_or_compra(obj: Dict[str, Any]) -> Optional[str]:
    return normalize_str(obj.get("numeroItemPncp")) or normalize_str(obj.get("numeroItemCompra"))

ID_RE = re.compile(r"^\d{4,}$")  # ajuste conforme formato esperado

def ler_ids(gc: gspread.Client) -> List[str]:
    """
    Lê apenas a coluna 'idCompra' de forma eficiente:
    - localiza cabeçalho na primeira linha
    - lê a coluna inteira
    - normaliza, remove vazios e duplicados
    """
    ws = gc.open_by_key(PLANILHA_ID).worksheet(ABA_SHEETS)
    headers = ws.row_values(1)
    header_map = {h.strip(): idx + 1 for idx, h in enumerate(headers)}
    if "idCompra" not in header_map:
        raise RuntimeError("A aba 'Buscas' deve conter a coluna 'idCompra'.")
    col = header_map["idCompra"]
    values = ws.col_values(col)[1:]  # pula header
    ids = [normalize_str(v) for v in values if normalize_str(v)]
    ids = [i for i in ids if ID_RE.match(i)]
    return sorted(set(ids))

def doc_id_preco(id_compra: str, numero_item: str) -> str:
    return f"{id_compra}-{numero_item}"

# ============== Paginação PNCP (resultado + páginas) ==============
def fetch_paginated_resultado(session: requests.Session, endpoint: str, base_params: Dict[str, Any], max_pages: int = MAX_PAGES) -> List[Dict[str, Any]]:
    resultados: List[Dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        params = dict(base_params)
        params["pagina"] = page
        payload = get_json(session, endpoint, params)
        lista = payload.get("resultado") if isinstance(payload, dict) else None
        if not isinstance(lista, list) or not lista:
            break
        resultados.extend([x for x in lista if isinstance(x, dict)])

        total_paginas = payload.get("totalPaginas")
        paginas_rest = payload.get("paginasRestantes")
        if isinstance(total_paginas, int) and page >= total_paginas:
            break
        if isinstance(paginas_rest, int) and paginas_rest <= 0:
            break
        page += 1
    return resultados

# ============== Coleta (2.1 e 3.1 por idCompra) ==============
def get_itens_21_por_id(session: requests.Session, id_compra: str) -> List[Dict[str, Any]]:
    raw_list = fetch_paginated_resultado(session, EP_ITENS, {"tipo": "idCompra", "codigo": id_compra})
    itens: List[Dict[str, Any]] = []
    for raw in raw_list:
        raw = dict(raw)
        raw["numeroItemPncp"] = normalize_str(raw.get("numeroItemPncp"))
        raw["numeroItemCompra"] = normalize_str(raw.get("numeroItemCompra"))
        raw["codItemCatalogo"] = normalize_str(raw.get("codItemCatalogo"))
        doc = only_fields(raw, ITENS_KEEP)
        doc.setdefault("idCompra", id_compra)
        itens.append(doc)
    return itens

def get_resultados_31_por_id(session: requests.Session, id_compra: str) -> List[Dict[str, Any]]:
    raw_list = fetch_paginated_resultado(session, EP_RES, {"tipo": "idCompra", "codigo": id_compra})
    resultados: List[Dict[str, Any]] = []
    for raw in raw_list:
        raw = dict(raw)
        raw["numeroItemPncp"] = normalize_str(raw.get("numeroItemPncp"))
        raw["niFornecedor"]   = normalize_str(raw.get("niFornecedor"))
        doc = only_fields(raw, RESULTADOS_KEEP)
        doc.setdefault("idCompra", id_compra)
        resultados.append(doc)
    return resultados

# ============== CSV: decodificar corretamente e corrigir mojibake ==============
def _decode_csv_bytes(raw: bytes) -> str:
    """
    Decodifica os bytes do CSV sem perder acentos:
    - detecta encoding com charset-normalizer (sem 'ignore')
    - ftfy para consertar mojibake
    - fallback para latin-1/cp1252
    """
    try:
        probe = from_bytes(raw).best()
        if probe and probe.encoding:
            decoded_main = raw.decode(probe.encoding, errors="strict")
        else:
            decoded_main = raw.decode("utf-8", errors="strict")
    except Exception:
        try:
            decoded_main = raw.decode("latin-1", errors="strict")
        except Exception:
            decoded_main = raw.decode("cp1252", errors="strict")

    fixed_main = fix_text(decoded_main)

    def score_bad(s: str) -> int:
        # Remove espaço da contagem para evitar falso-positivo
        return sum(s.count(ch) for ch in ("Ã", "Â"))

    if score_bad(fixed_main) > 0:
        try:
            alt = decoded_main.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
            alt_fixed = fix_text(alt)
            if score_bad(alt_fixed) < score_bad(fixed_main):
                fixed_main = alt_fixed
        except Exception:
            pass

    return fixed_main

def _read_csv_bytes_to_df(raw: bytes) -> pd.DataFrame:
    decoded = _decode_csv_bytes(raw)
    # Detecta delimitador ; ou , com Sniffer
    sample = decoded[:4096]
    try:
        dialect = pycsv.Sniffer().sniff(sample, delimiters=";,")
        sep = dialect.delimiter
    except Exception:
        sep = ";"
    df = pd.read_csv(io.StringIO(decoded), sep=sep, dtype=str, na_filter=False, engine="python")
    df.columns = [str(c).strip() for c in df.columns]
    # mapa case-insensitive
    df.attrs["_lower_map"] = {c.lower(): c for c in df.columns}
    return df

def _get_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower_map = df.attrs.get("_lower_map", {c.lower(): c for c in df.columns})
    for c in candidates:
        real = lower_map.get(c.lower())
        if real:
            return real
    return None

def _row_to_precos_payload(row: Dict[str, Any], colmap: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for target_key, real_col in colmap.items():
        if real_col:
            val = row.get(real_col)
            out[target_key] = None if (val is None or str(val).strip() == "") else str(val).strip()
        else:
            out[target_key] = None
    return out

# ============== Utilitários Firestore (chunks) ==============
def _commit_in_chunks(writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]], db: firestore.Client, chunk_size: int = BATCH_CHUNK_SIZE):
    if DRY_RUN or not writes:
        return
    for i in range(0, len(writes), chunk_size):
        batch = db.batch()
        for ref, payload in writes[i:i+chunk_size]:
            batch.set(ref, payload, merge=True)
        batch.commit()

def _get_all_chunked(db: firestore.Client, refs: List[firestore.DocumentReference], chunk: int = GET_ALL_CHUNK):
    snaps = []
    for i in range(0, len(refs), chunk):
        snaps.extend(db.get_all(refs[i:i+chunk]))
    return snaps

# ============== Cache Firestore para PESQUISA_PRECOS ==============
def preco_docs_existentes(db: firestore.Client, id_compra: str, item_nums: Set[str]) -> Set[str]:
    if not item_nums:
        return set()
    refs = [db.collection(COL_PRECO).document(doc_id_preco(id_compra, n)) for n in item_nums]
    existing: Set[str] = set()
    for snap in _get_all_chunked(db, refs):
        if snap.exists:
            existing.add(snap.id)
    return existing

def carregar_cache_precos_existentes(db: firestore.Client, id_compra: str, item_nums: Set[str]) -> Dict[str, Dict[str, Any]]:
    if not item_nums:
        return {}
    refs = [db.collection(COL_PRECO).document(doc_id_preco(id_compra, n)) for n in item_nums]
    mapa: Dict[str, Dict[str, Any]] = {}
    for snap in _get_all_chunked(db, refs):
        if snap.exists:
            num_item = snap.id.split("-", 1)[1] if "-" in snap.id else None
            if num_item:
                mapa[num_item] = snap.to_dict()
    return mapa

# ============== Baixar CSV só para itens faltantes e gravar (filtrado por UASG) ==============
def fetch_and_upsert_pesquisa_precos(
    db: firestore.Client,
    session: requests.Session,
    id_compra: str,
    itens: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    item_nums_all: Set[str] = {pick_num_item_pncp_or_compra(it) for it in itens}
    item_nums_all = {n for n in item_nums_all if n}
    codigos: Set[str] = {normalize_str(it.get("codItemCatalogo")) for it in itens if it.get("codItemCatalogo")}
    codigos = {c for c in codigos if c}

    exists = preco_docs_existentes(db, id_compra, item_nums_all)
    ja_tem: Set[str] = {doc_id.split("-", 1)[1] for doc_id in exists if "-" in doc_id}
    faltantes: Set[str] = item_nums_all - ja_tem

    preco_map: Dict[str, Dict[str, Any]] = carregar_cache_precos_existentes(db, id_compra, item_nums_all)

    if not faltantes or not codigos:
        return preco_map

    for cod in sorted(codigos):
        if not faltantes:
            break
        tried_params = [
            {"codigoItemCatalogo": cod},
            {"tipo": "codigoItemCatalogo", "codigo": cod},
        ]
        raw = None
        for params in tried_params:
            try:
                raw = get_bytes(session, EP_CSV, params)
                break
            except requests.HTTPError:
                continue
        if not raw:
            continue

        df = _read_csv_bytes_to_df(raw)

        real_cols: Dict[str, Optional[str]] = {}
        for target, candidates in PRECO_FIELD_CANDIDATES.items():
            real_cols[target] = _get_col(df, candidates)

        col_idcompra = real_cols.get("idCompra")
        col_numitem  = real_cols.get("numeroItemCompra")
        col_uasg     = real_cols.get("codigoUasg")
        if not col_idcompra or not col_numitem or not col_uasg:
            log.info(f"[PREÇOS] {id_compra}: CSV para codItemCatalogo {cod} sem colunas mínimas. Pulando.")
            continue

        dff = df[
            (df[col_idcompra].astype(str).str.strip() == str(id_compra).strip())
            &
            (df[col_numitem].astype(str).str.strip().isin({x.strip() for x in faltantes}))
            &
            (df[col_uasg].astype(str).str.strip() == str(UASG_ALVO).strip())
        ]
        if dff.empty:
            continue

        writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]] = []
        novos_ids: Set[str] = set()
        for _, row in dff.iterrows():
            row_dict = row.to_dict()
            payload = _row_to_precos_payload(row_dict, {k: (v or "") for k, v in real_cols.items()})
            num_item = payload.get("numeroItemCompra")
            if not num_item:
                continue
            payload["updatedAt"] = firestore.SERVER_TIMESTAMP
            ref = db.collection(COL_PRECO).document(doc_id_preco(id_compra, num_item))
            writes.append((ref, payload))
            preco_map[num_item] = payload
            novos_ids.add(num_item)

        _commit_in_chunks(writes, db)
        faltantes -= novos_ids
        log.info(f"[PREÇOS] {id_compra}: codItemCatalogo {cod} -> upsert {len(novos_ids)}")

    return preco_map

# ============== Persistência (upsert) ==============
def upsert_itens(db: firestore.Client, itens: List[Dict[str, Any]]):
    if not itens:
        return
    writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]] = []
    for it in itens:
        id_compra = it.get("idCompra")
        num_item  = normalize_str(it.get("numeroItemPncp"))
        if not id_compra or not num_item:
            continue
        it_clean = only_fields(it, ITENS_KEEP)
        it_clean["updatedAt"] = firestore.SERVER_TIMESTAMP
        ref = db.collection(COL_ITENS).document(f"{id_compra}-{num_item}")
        writes.append((ref, it_clean))
    _commit_in_chunks(writes, db)

def upsert_resultados(db: firestore.Client, resultados: List[Dict[str, Any]]):
    if not resultados:
        return
    writes: List[Tuple[firestore.DocumentReference, Dict[str, Any]]] = []
    for r in resultados:
        id_compra = r.get("idCompra")
        num_item  = normalize_str(r.get("numeroItemPncp"))
        ni        = normalize_str(r.get("niFornecedor"))
        if not id_compra or not num_item or not ni:
            continue
        r_clean = only_fields(r, RESULTADOS_KEEP)
        r_clean["updatedAt"] = firestore.SERVER_TIMESTAMP
        ref = db.collection(COL_RES).document(f"{id_compra}-{num_item}-{ni}")
        writes.append((ref, r_clean))
    _commit_in_chunks(writes, db)

# ============== Pipeline ==============
def processar():
    creds_sheets = build_sheets_credentials()
    creds_fs = build_firestore_credentials()

    gc = make_sheets_client(creds_sheets)
    db = make_firestore_client(creds_fs)
    session = make_session()

    log.info(f"Firestore conectado no projeto: {db.project}")
    log.info(f"PLANILHA_ID={PLANILHA_ID} ABA_SHEETS={ABA_SHEETS} UASG_ALVO={UASG_ALVO} DRY_RUN={DRY_RUN}")

    ids = ler_ids(gc)
    log.info(f"IDs a processar: {ids}")

    for idc in ids:
        try:
            log.info(f"==> {idc}")
            # 1) 2.1 Itens
            itens = get_itens_21_por_id(session, idc)
            log.info(f"[2.1] {idc}: itens = {len(itens)}")

            # 2) PESQUISA_PRECOS (cache + fetch faltantes com filtro UASG)
            _ = fetch_and_upsert_pesquisa_precos(db, session, idc, itens)

            # 3) 3.1 Resultados
            resultados = get_resultados_31_por_id(session, idc)
            log.info(f"[3.1] {idc}: resultados = {len(resultados)}")

            # 4) Persistir ITENS e RESULTADOS
            upsert_itens(db, itens)
            upsert_resultados(db, resultados)

        except requests.HTTPError as e:
            body = e.response.text[:500] if e.response is not None else ""
            log.error(f"HTTPError {idc}: status={getattr(e.response,'status_code',None)} body={body}")
        except Exception as e:
            log.exception(f"Falha em {idc}: {e}")

if __name__ == "__main__":
    processar()