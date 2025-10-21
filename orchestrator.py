from __future__ import annotations
import os, json
from typing import List, Tuple
from sheets_io import read_rows_ordered, batch_write_status, iso_utc_now
import script as pncp  # importa seu script.py como módulo

PLANILHA_ID = os.environ.get("PLANILHA_ID")
ABA_SHEETS = os.environ.get("ABA_SHEETS", "Página1")
GCP_SHEETS_SA_JSON = os.environ.get("GCP_SHEETS_SA_JSON")
GCP_SHEETS_SA_PATH = os.environ.get("GCP_SHEETS_SA_PATH")

def _sa_info():
    return json.loads(GCP_SHEETS_SA_JSON) if GCP_SHEETS_SA_JSON else None

def run():
    rows = read_rows_ordered(PLANILHA_ID, ABA_SHEETS, sa_info_json=_sa_info(), sa_file_path=GCP_SHEETS_SA_PATH)
    updates: List[Tuple[int, str, str]] = []

    for r in rows:
        idc = r["idCompra"]
        if not idc:
            continue
        res = pncp.run_once(idc)
        itens = int(res.get("itens_encontrados", 0))
        resultados = int(res.get("resultados_encontrados", 0))
        mudou = bool(res.get("houve_modificacao", False))
        status = (
            f"{itens} itens encontrados; {resultados} resultados encontrados; "
            + ("Dados atualizados no Banco" if mudou else "Sem modificações desde a última busca")
        )
        updates.append((r["row_number"], iso_utc_now(), status))

    if updates:
        batch_write_status(PLANILHA_ID, ABA_SHEETS, updates, sa_info_json=_sa_info(), sa_file_path=GCP_SHEETS_SA_PATH)

if __name__ == "__main__":
    run()