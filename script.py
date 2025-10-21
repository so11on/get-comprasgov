/**
 * Processa as consultas à API na ordem exigida e registra data/hora da última consulta na coluna G.
 * Requisitos atendidos:
 * 1) Escreve data e hora da última consulta na coluna G (uma por linha processada).
 * 2) Ordem de execução: primeiro linhas com G em branco; depois por G em ordem crescente (mais antiga -> mais nova).
 *
 * Observações:
 * - A planilha (aba) alvo deve se chamar exatamente "dataBusca".
 * - A coluna G é reservada para o carimbo de data/hora da última consulta à API.
 * - Ajuste as funções buildPayloadFromRow e tratarRespostaDaApi conforme seu layout/uso real.
 */

function processarConsultas() {
  const NOME_ABA = 'dataBusca';
  const COL_TS = 7; // Coluna G
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName(NOME_ABA);
  if (!sh) throw new Error('Aba "dataBusca" não encontrada.');

  const tz = ss.getSpreadsheetTimeZone();
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 2) return; // nada para processar (apenas cabeçalho)

  // Lê todos os dados (exclui cabeçalho)
  const range = sh.getRange(2, 1, lastRow - 1, lastCol);
  const values = range.getValues();

  // Monta vetor com índices para manter posição original sem reordenar a planilha
  const tsIdx = COL_TS - 1; // índice 0-based da coluna G
  const linhas = values.map((row, i) => ({ idx: i, row }));

  // Ordenação: 1) primeiro G em branco; 2) depois por G crescente (mais antiga -> mais nova)
  linhas.sort((a, b) => {
    const va = a.row[tsIdx];
    const vb = b.row[tsIdx];


    if (aBlank && !bBlank) return -1;
    if (!aBlank && bBlank) return 1;
    if (aBlank && bBlank) return 0;

    // Ambos preenchidos: comparar como datas
    const da = va instanceof Date ? va : new Date(va);
    const db = vb instanceof Date ? vb : new Date(vb);

    // Caso haja valores inválidos, empurra os inválidos para o final
    if (isNaN(da) && isNaN(db)) return 0;
    if (isNaN(da)) return 1;
    if (isNaN(db)) return -1;

    return da - db;
  });

  // Processa na ordem determinada, mas grava cada resultado na sua linha original
  for (const item of linhas) {
    const { idx, row } = item;

    // Monte o payload/entrada para a API a partir da linha
    const payload = buildPayloadFromRow(row);

    // Chame a API (implemente conforme sua necessidade)
    const resposta = chamarApi(payload);

    // Trate a resposta (escreva nas colunas apropriadas, se houver)
    tratarRespostaDaApi(row, resposta);

    // Carimbo de data/hora da consulta na coluna G (data/hora local da planilha)
    const stamp = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
    row[tsIdx] = stamp;

    // Grava a linha completa de volta na posição original
    sh.getRange(idx + 2, 1, 1, lastCol).setValues([row]);

    // Opcional: breve pausa para evitar limites de taxa de API
    Utilities.sleep(100);
  }
}

/**
 * Constrói o payload para a API a partir dos dados da linha.
 * Ajuste esta função ao seu layout real da planilha.
 */
function buildPayloadFromRow(row) {
  // EXEMPLO: supondo que as colunas A, B, C tenham dados necessários
  // A (0), B (1), C (2)...
  return {
    campoA: row[0],
    campoB: row[1],
    campoC: row[2],
  };
}

/**
 * Chamada genérica à API.
 * Substitua URL, método, headers e body conforme a sua API.
 */
function chamarApi(payload) {
  const url = 'https://sua.api.exemplo/endpoint';
  const options = {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    payload: JSON.stringify(payload),
  };

  const resp = UrlFetchApp.fetch(url, options);
  const code = resp.getResponseCode();
    // Lide com erro HTTP conforme necessidade
    return { erro: true, status: code, corpo: resp.getContentText() };
  }
  try {
    return JSON.parse(resp.getContentText());
  } catch (e) {
    return { erro: true, status: code, corpo: resp.getContentText() };
  }
}

/**
 * Atualiza a linha com dados retornados da API.
 * Ajuste mapeamentos de colunas conforme necessidade.
 */
function tratarRespostaDaApi(row, resposta) {
  if (resposta && !resposta.erro) {
    // EXEMPLO: escrever resultados em colunas D (3), E (4), F (5)
    row[3] = resposta.resultadoD ?? row[3];
    row[4] = resposta.resultadoE ?? row[4];
    row[5] = resposta.resultadoF ?? row[5];
  } else {
    // Se quiser marcar erro em alguma coluna, faça aqui
    // EXEMPLO: coluna F marca mensagem de erro
    const msg = resposta && resposta.corpo ? String(resposta.corpo).slice(0, 500) : 'Erro na chamada da API';
    row[5] = msg;
  }
}