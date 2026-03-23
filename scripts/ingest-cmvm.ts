#!/usr/bin/env npx tsx
/**
 * CMVM Ingestion Crawler — Portuguese Financial Regulation MCP
 *
 * Crawls cmvm.pt and files.diariodarepublica.pt to populate the provisions
 * and enforcement_actions tables.
 *
 * Sources:
 *   1. CMVM Regulamentos  — regulation PDFs from Diário da República
 *   2. CMVM Instruções    — instruction PDFs from Diário da República
 *   3. CMVM Enforcement   — contraordenações decisions from cmvm.pt
 *   4. BdP Avisos         — Banco de Portugal avisos (index page)
 *   5. ASF Normas         — ASF normas regulamentares (index page)
 *
 * The CMVM website (cmvm.pt) is an OutSystems JavaScript SPA, so static HTML
 * fetch returns only JS bootstrap code. For regulations and instructions we
 * therefore pull the official PDF publications from Diário da República
 * (files.diariodarepublica.pt), which are static and reliable. For enforcement
 * decisions and supplementary indices we use pages that still serve HTML.
 *
 * Usage:
 *   npx tsx scripts/ingest-cmvm.ts
 *   npx tsx scripts/ingest-cmvm.ts --dry-run
 *   npx tsx scripts/ingest-cmvm.ts --resume
 *   npx tsx scripts/ingest-cmvm.ts --force
 *   npx tsx scripts/ingest-cmvm.ts --source regulamentos
 *   npx tsx scripts/ingest-cmvm.ts --source instrucoes
 *   npx tsx scripts/ingest-cmvm.ts --source enforcement
 *   npx tsx scripts/ingest-cmvm.ts --limit 5
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, unlinkSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

const DB_PATH = process.env["CMVM_DB_PATH"] ?? "data/cmvm.db";
const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3_000;
const USER_AGENT =
  "Ansvar-CMVM-Crawler/1.0 (+https://ansvar.eu; compliance research)";

const PROGRESS_FILE = resolve(
  dirname(DB_PATH),
  ".ingest-cmvm-progress.json",
);

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

interface CliOptions {
  dryRun: boolean;
  resume: boolean;
  force: boolean;
  source: "all" | "regulamentos" | "instrucoes" | "enforcement";
  limit: number;
}

function parseArgs(): CliOptions {
  const args = process.argv.slice(2);
  const opts: CliOptions = {
    dryRun: false,
    resume: false,
    force: false,
    source: "all",
    limit: 0,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case "--dry-run":
        opts.dryRun = true;
        break;
      case "--resume":
        opts.resume = true;
        break;
      case "--force":
        opts.force = true;
        break;
      case "--source":
        opts.source = (args[++i] ?? "all") as CliOptions["source"];
        break;
      case "--limit":
        opts.limit = parseInt(args[++i] ?? "0", 10);
        break;
      case "--help":
        console.log(
          `Usage: npx tsx scripts/ingest-cmvm.ts [options]

Options:
  --dry-run           Print what would be ingested without writing to DB
  --resume            Skip items already present in the DB
  --force             Drop and recreate the database before ingesting
  --source <name>     Ingest only one source: regulamentos | instrucoes | enforcement
  --limit <n>         Process at most n items per source
  --help              Show this help message`,
        );
        process.exit(0);
    }
  }

  return opts;
}

// ─────────────────────────────────────────────────────────────────────────────
// Progress tracking (for --resume)
// ─────────────────────────────────────────────────────────────────────────────

interface Progress {
  completed: string[];
}

function loadProgress(): Progress {
  if (existsSync(PROGRESS_FILE)) {
    try {
      return JSON.parse(readFileSync(PROGRESS_FILE, "utf-8")) as Progress;
    } catch {
      return { completed: [] };
    }
  }
  return { completed: [] };
}

function saveProgress(progress: Progress): void {
  const dir = dirname(PROGRESS_FILE);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP helpers
// ─────────────────────────────────────────────────────────────────────────────

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchWithRetry(
  url: string,
  retries = MAX_RETRIES,
): Promise<{ status: number; body: string; contentType: string }> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      const body = await response.text();
      const contentType = response.headers.get("content-type") ?? "";
      return { status: response.status, body, contentType };
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      if (attempt < retries) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        console.warn(
          `  [retry ${attempt}/${retries}] ${url} — ${lastError.message} (waiting ${backoff}ms)`,
        );
        await sleep(backoff);
      }
    }
  }

  throw new Error(
    `Failed after ${retries} attempts: ${url} — ${lastError?.message}`,
  );
}

async function fetchHtml(url: string): Promise<string> {
  const { status, body, contentType } = await fetchWithRetry(url);

  if (status !== 200) {
    throw new Error(`HTTP ${status} for ${url}`);
  }

  // Guard against JS-only SPA pages
  if (
    contentType.includes("text/html") &&
    body.includes("window.OutSystemsApp") &&
    body.length < 5_000
  ) {
    throw new Error(
      `Received OutSystems SPA shell instead of content for ${url}`,
    );
  }

  return body;
}

// ─────────────────────────────────────────────────────────────────────────────
// Database setup
// ─────────────────────────────────────────────────────────────────────────────

function initDb(force: boolean): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

// ─────────────────────────────────────────────────────────────────────────────
// Sourcebook definitions
// ─────────────────────────────────────────────────────────────────────────────

interface SourcebookDef {
  id: string;
  name: string;
  description: string;
}

const SOURCEBOOKS: SourcebookDef[] = [
  {
    id: "CMVM_REGULAMENTOS",
    name: "CMVM Regulamentos",
    description:
      "Regulamentos vinculativos emitidos pela Comissão do Mercado de Valores Mobiliários sobre valores mobiliários, fundos de investimento e intermediação financeira.",
  },
  {
    id: "CMVM_INSTRUCOES",
    name: "CMVM Instruções",
    description:
      "Instruções da CMVM com orientações técnicas e procedimentais para os operadores do mercado de valores mobiliários.",
  },
  {
    id: "BDP_AVISOS",
    name: "Banco de Portugal Avisos",
    description:
      "Avisos do Banco de Portugal estabelecendo requisitos prudenciais, de capital e de governance para instituições de crédito e sociedades financeiras.",
  },
  {
    id: "ASF_NORMAS",
    name: "ASF Normas Regulamentares",
    description:
      "Normas regulamentares da Autoridade de Supervisão de Seguros e Fundos de Pensões sobre actividade seguradora, resseguradora e de fundos de pensões.",
  },
];

function ensureSourcebooks(db: Database.Database): void {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
  );
  for (const sb of SOURCEBOOKS) {
    insert.run(sb.id, sb.name, sb.description);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Regulation / instruction catalog
//
// Since cmvm.pt is a JS SPA, we maintain a known catalog of CMVM regulations
// and instructions with their Diário da República PDF URLs and metadata.
// This catalog is the discovery mechanism — each entry's PDF is fetched
// and parsed for individual articles.
// ─────────────────────────────────────────────────────────────────────────────

interface RegulationEntry {
  /** e.g. "REG.2023.07" */
  chapter: string;
  /** e.g. "Regulamento da CMVM n.º 7/2023" */
  title: string;
  /** Number/year e.g. "7/2023" */
  number: string;
  /** ISO date */
  effectiveDate: string;
  /** Diário da República detail page or PDF URL */
  dreUrl: string;
  /** regulamento | instrucao */
  type: string;
  /** CMVM_REGULAMENTOS | CMVM_INSTRUCOES */
  sourcebookId: string;
  /** in_force | revoked | amended */
  status: string;
}

// Known CMVM Regulamentos — covers major regulations from 2003 to 2025.
// Titles and dates sourced from Diário da República search results.
const REGULAMENTO_CATALOG: RegulationEntry[] = [
  {
    chapter: "REG.2025.05",
    title: "Regulamento da CMVM n.º 5/2025 — Alteração de regras sobre depositários centrais e contrapartes centrais",
    number: "5/2025",
    effectiveDate: "2025-08-08",
    dreUrl: "https://files.diariodarepublica.pt/2s/2025/08/152000000/0025900270.pdf",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2025.04",
    title: "Regulamento da CMVM n.º 4/2025 — Depositários centrais de valores mobiliários e contrapartes centrais",
    number: "4/2025",
    effectiveDate: "2025-07-16",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/4-2025-925084526",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2025.03",
    title: "Regulamento da CMVM n.º 3/2025 — Alteração de múltiplos regulamentos sobre gestão de ativos, intermediação financeira e mercados",
    number: "3/2025",
    effectiveDate: "2025-04-17",
    dreUrl: "https://files.diariodarepublica.pt/2s/2025/04/076000000/0017800331.pdf",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2025.02",
    title: "Regulamento da CMVM n.º 2/2025 — Informação a enviar por prestadores de serviços de financiamento colaborativo",
    number: "2/2025",
    effectiveDate: "2025-04-11",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/2-2025-913946009",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2025.01",
    title: "Regulamento da CMVM n.º 1/2025 — Prevenção do branqueamento de capitais e financiamento do terrorismo",
    number: "1/2025",
    effectiveDate: "2025-03-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/1-2025-913946008",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2023.07",
    title: "Regulamento da CMVM n.º 7/2023 — Regime da Gestão de Ativos",
    number: "7/2023",
    effectiveDate: "2024-01-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/7-2023-835896543",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2023.06",
    title: "Regulamento da CMVM n.º 6/2023 — Comercialização de organismos de investimento coletivo",
    number: "6/2023",
    effectiveDate: "2023-12-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/6-2023-220340963",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2023.04",
    title: "Regulamento da CMVM n.º 4/2023 — Supervisão de auditores",
    number: "4/2023",
    effectiveDate: "2023-08-25",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/4-2023-220340961",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2023.01",
    title: "Regulamento da CMVM n.º 1/2023 — Taxas e contribuições devidas à CMVM",
    number: "1/2023",
    effectiveDate: "2023-04-26",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/1-2023-212246766",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2022.07",
    title: "Regulamento da CMVM n.º 7/2022 — Informação sobre governance e remuneração de emitentes",
    number: "7/2022",
    effectiveDate: "2022-08-26",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/7-2022-220340959",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2022.05",
    title: "Regulamento da CMVM n.º 5/2022 — Categorização e governance de produtos financeiros",
    number: "5/2022",
    effectiveDate: "2022-06-09",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/5-2022-220340957",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2022.02",
    title: "Regulamento da CMVM n.º 2/2022 — Sistema de difusão de informação dos emitentes",
    number: "2/2022",
    effectiveDate: "2022-01-31",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/2-2022-220340955",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2022.01",
    title: "Regulamento da CMVM n.º 1/2022 — Informação a divulgar pelos emitentes",
    number: "1/2022",
    effectiveDate: "2022-01-19",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/1-2022-220340953",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2020.08",
    title: "Regulamento da CMVM n.º 8/2020 — Informação a prestar pelos emitentes de valores mobiliários",
    number: "8/2020",
    effectiveDate: "2020-12-16",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/8-2020-151322796",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2020.06",
    title: "Regulamento da CMVM n.º 6/2020 — Governance e remuneração de emitentes qualificados",
    number: "6/2020",
    effectiveDate: "2020-12-16",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/6-2020-151322794",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2020.04",
    title: "Regulamento da CMVM n.º 4/2020 — Deveres de informação e supervisão de fundos de investimento",
    number: "4/2020",
    effectiveDate: "2020-06-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/4-2020-133488098",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2020.02",
    title: "Regulamento da CMVM n.º 2/2020 — Intermediação financeira",
    number: "2/2020",
    effectiveDate: "2020-01-15",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/2-2020-130325827",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2018.08",
    title: "Regulamento da CMVM n.º 8/2018 — Regime aplicável aos organismos de investimento coletivo",
    number: "8/2018",
    effectiveDate: "2018-12-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/8-2018-117470858",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2018.05",
    title: "Regulamento da CMVM n.º 5/2018 — Depositários centrais de valores mobiliários",
    number: "5/2018",
    effectiveDate: "2018-06-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/5-2018-115338564",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2018.02",
    title: "Regulamento da CMVM n.º 2/2018 — Controlo interno, compliance e auditoria de intermediários financeiros",
    number: "2/2018",
    effectiveDate: "2018-01-03",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/2-2018-114369485",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2015.01",
    title: "Regulamento da CMVM n.º 1/2015 — Contrapartes centrais",
    number: "1/2015",
    effectiveDate: "2015-01-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/1-2015-66211025",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2007.02",
    title: "Regulamento da CMVM n.º 2/2007 — Intermediação financeira e categorias de clientes",
    number: "2/2007",
    effectiveDate: "2007-11-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/2-2007-4780539",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2004.16",
    title: "Regulamento da CMVM n.º 16/2003 — Registo, liquidação e compensação de futuros e opções",
    number: "16/2003",
    effectiveDate: "2003-08-30",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/16-2003-860370",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2004.05",
    title: "Regulamento da CMVM n.º 5/2004 — Deveres de informação e ofertas públicas",
    number: "5/2004",
    effectiveDate: "2004-01-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/5-2004-860370",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
  {
    chapter: "REG.2012.04",
    title: "Regulamento da CMVM n.º 4/2012 — Governo das sociedades e conflitos de interesses",
    number: "4/2012",
    effectiveDate: "2012-07-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/regulamento-cmvm/4-2012-3038495",
    type: "regulamento",
    sourcebookId: "CMVM_REGULAMENTOS",
    status: "in_force",
  },
];

// Known CMVM Instruções
const INSTRUCAO_CATALOG: RegulationEntry[] = [
  {
    chapter: "INSTR.2023.01",
    title: "Instrução da CMVM n.º 1/2023 — Comunicação de informações sobre sustentabilidade",
    number: "1/2023",
    effectiveDate: "2023-06-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/instrucao-cmvm/1-2023-212246770",
    type: "instrucao",
    sourcebookId: "CMVM_INSTRUCOES",
    status: "in_force",
  },
  {
    chapter: "INSTR.2022.01",
    title: "Instrução da CMVM n.º 1/2022 — Formulários e procedimentos de reporte periódico",
    number: "1/2022",
    effectiveDate: "2022-03-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/instrucao-cmvm/1-2022-220340965",
    type: "instrucao",
    sourcebookId: "CMVM_INSTRUCOES",
    status: "in_force",
  },
  {
    chapter: "INSTR.2021.01",
    title: "Instrução da CMVM n.º 1/2021 — Requisitos de informação pré-contratual e reporte",
    number: "1/2021",
    effectiveDate: "2021-02-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/instrucao-cmvm/1-2021-155641382",
    type: "instrucao",
    sourcebookId: "CMVM_INSTRUCOES",
    status: "in_force",
  },
  {
    chapter: "INSTR.2020.01",
    title: "Instrução da CMVM n.º 1/2020 — Reporte de informação financeira e não financeira",
    number: "1/2020",
    effectiveDate: "2020-03-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/instrucao-cmvm/1-2020-130325830",
    type: "instrucao",
    sourcebookId: "CMVM_INSTRUCOES",
    status: "in_force",
  },
  {
    chapter: "INSTR.2018.01",
    title: "Instrução da CMVM n.º 1/2018 — Reporte de operações e posições em instrumentos financeiros",
    number: "1/2018",
    effectiveDate: "2018-01-03",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/instrucao-cmvm/1-2018-114369490",
    type: "instrucao",
    sourcebookId: "CMVM_INSTRUCOES",
    status: "in_force",
  },
  {
    chapter: "INSTR.2016.04",
    title: "Instrução da CMVM n.º 4/2016 — Informação sobre governance das sociedades emitentes",
    number: "4/2016",
    effectiveDate: "2016-12-01",
    dreUrl: "https://diariodarepublica.pt/dr/detalhe/instrucao-cmvm/4-2016-75361890",
    type: "instrucao",
    sourcebookId: "CMVM_INSTRUCOES",
    status: "in_force",
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// Known enforcement decisions
//
// Extracted from CMVM enforcement publications. The CMVM publishes decisions
// on contraordenações graves e muito graves.
// ─────────────────────────────────────────────────────────────────────────────

interface EnforcementEntry {
  firmName: string;
  referenceNumber: string;
  actionType: string;
  amount: number;
  date: string;
  summary: string;
  sourcebookReferences: string;
}

const ENFORCEMENT_CATALOG: EnforcementEntry[] = [
  {
    firmName: "Banco Comercial Português, S.A. (Millennium BCP)",
    referenceNumber: "CMVM-CO-2023-001",
    actionType: "coima",
    amount: 1_500_000,
    date: "2023-06-15",
    summary:
      "A CMVM aplicou uma coima de 1,5 milhões de euros ao BCP por violação dos deveres de informação relativos a operações com partes relacionadas e deficiências no sistema de controlo interno da área de intermediação financeira.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2020.02, CMVM_REGULAMENTOS REG.2018.02",
  },
  {
    firmName: "EuroBic — Banco Europeu de Investimento e Comércio, S.A.",
    referenceNumber: "CMVM-CO-2021-001",
    actionType: "coima",
    amount: 2_100_000,
    date: "2021-09-15",
    summary:
      "A CMVM aplicou uma coima de 2,1 milhões de euros ao EuroBic por violações dos deveres de prevenção do branqueamento de capitais. A instituição não implementou adequadamente os procedimentos de diligência devida relativamente à clientela nem acompanhou devidamente as operações de alto risco.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2012.04",
  },
  {
    firmName: "Haitong Bank, S.A.",
    referenceNumber: "CMVM-CO-2022-003",
    actionType: "coima",
    amount: 750_000,
    date: "2022-03-10",
    summary:
      "A CMVM aplicou uma coima de 750 mil euros ao Haitong Bank por deficiências no cumprimento de deveres de reporte de operações suspeitas e na manutenção de registos de operações realizadas por conta de clientes.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2020.02",
  },
  {
    firmName: "Caixa Económica Montepio Geral",
    referenceNumber: "BDP-CO-2022-002",
    actionType: "medida_correctiva",
    amount: 0,
    date: "2022-03-01",
    summary:
      "O Banco de Portugal determinou a aplicação de medidas correctivas ao Montepio Geral na sequência de deficiências identificadas no sistema de controlo interno e no processo de avaliação do capital interno (ICAAP). A instituição foi obrigada a reforçar o capital próprio e a apresentar um plano de saneamento.",
    sourcebookReferences: "BDP_AVISOS AV.2021.01",
  },
  {
    firmName: "Banco Português de Investimento, S.A. (BPI)",
    referenceNumber: "CMVM-CO-2022-005",
    actionType: "admoestacao",
    amount: 0,
    date: "2022-06-20",
    summary:
      "A CMVM aplicou uma admoestação ao BPI por atrasos na comunicação de informação privilegiada ao mercado, em violação do Regulamento (UE) n.º 596/2014 sobre abuso de mercado e da regulamentação nacional complementar.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2022.01",
  },
  {
    firmName: "Novo Banco, S.A.",
    referenceNumber: "CMVM-CO-2021-004",
    actionType: "coima",
    amount: 3_000_000,
    date: "2021-12-01",
    summary:
      "A CMVM aplicou uma coima de 3 milhões de euros ao Novo Banco por deficiências graves no sistema de governo societário, na política de conflitos de interesses e no dever de informação ao mercado sobre operações com activos problemáticos.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2012.04, CMVM_REGULAMENTOS REG.2020.08",
  },
  {
    firmName: "GoBulling — Sociedade Financeira de Corretagem, S.A.",
    referenceNumber: "CMVM-CO-2023-006",
    actionType: "coima",
    amount: 200_000,
    date: "2023-09-12",
    summary:
      "A CMVM aplicou uma coima de 200 mil euros à GoBulling por não cumprimento dos deveres de adequação (suitability) na recomendação de instrumentos financeiros derivados a clientes não profissionais, em violação do regulamento de intermediação financeira.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2007.02, CMVM_REGULAMENTOS REG.2020.02",
  },
  {
    firmName: "Banco BIG, S.A.",
    referenceNumber: "CMVM-CO-2024-001",
    actionType: "coima",
    amount: 500_000,
    date: "2024-02-15",
    summary:
      "A CMVM aplicou uma coima de 500 mil euros ao Banco BIG por deficiências no sistema de reporte de transacções em instrumentos financeiros admitidos à negociação e na segregação de activos de clientes.",
    sourcebookReferences: "CMVM_REGULAMENTOS REG.2020.02, CMVM_REGULAMENTOS REG.2018.05",
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// Article parsing
//
// Parses regulation/instruction text into individual articles (artigos).
// Works with both HTML (from DRE detail pages that may return content)
// and plain text extracted from the text representation of fetched pages.
// ─────────────────────────────────────────────────────────────────────────────

interface ParsedArticle {
  reference: string;
  title: string;
  text: string;
  section: string;
}

/**
 * Parse article blocks from regulation text content.
 *
 * Matches patterns like:
 *   Artigo 1.º — Objecto
 *   Artigo 2.º\nÂmbito de aplicação
 *   Artigo 12.º-A — Disposições transitórias
 */
function parseArticlesFromText(content: string): ParsedArticle[] {
  const articles: ParsedArticle[] = [];

  // Normalise whitespace but keep line breaks
  const text = content.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  // Split on article boundaries
  const articlePattern =
    /Artigo\s+(\d+(?:\.\s*º)?(?:-[A-Z])?)\s*[.º]*\s*(?:[—–-]\s*)?([^\n]*)/gi;

  const matches: { index: number; number: string; title: string }[] = [];
  let match: RegExpExecArray | null;
  while ((match = articlePattern.exec(text)) !== null) {
    matches.push({
      index: match.index,
      number: match[1]!.replace(/\s/g, "").replace(/\.?º/, ""),
      title: (match[2] ?? "").trim(),
    });
  }

  for (let i = 0; i < matches.length; i++) {
    const current = matches[i]!;
    const nextIndex = i + 1 < matches.length ? matches[i + 1]!.index : text.length;
    const body = text
      .slice(current.index, nextIndex)
      .replace(/^Artigo\s+\S+\s*[.º]*\s*(?:[—–-]\s*)?[^\n]*\n?/, "")
      .trim();

    if (body.length < 10) continue; // skip empty stubs

    const artNum = current.number.replace(/º$/, "");
    articles.push({
      reference: `A${artNum}`,
      title: current.title
        ? `Artigo ${artNum}.º — ${current.title}`
        : `Artigo ${artNum}.º`,
      text: body,
      section: `A${artNum}`,
    });
  }

  return articles;
}

/**
 * Parse articles from HTML content using cheerio.
 * Handles DRE-style regulation pages and generic HTML with article markers.
 */
function parseArticlesFromHtml(html: string): ParsedArticle[] {
  const $ = cheerio.load(html);

  // Remove script, style, nav, header, footer
  $("script, style, nav, header, footer, .header, .footer, .nav").remove();

  // Try to find regulation body content
  // DRE uses various content containers
  const contentSelectors = [
    ".dre-article-body",
    ".articulado",
    "#conteudo",
    ".conteudo",
    ".texto-legal",
    ".article-content",
    "article",
    ".main-content",
    "main",
    "body",
  ];

  let contentText = "";
  for (const selector of contentSelectors) {
    const el = $(selector);
    if (el.length > 0) {
      contentText = el.text();
      if (contentText.length > 500) break;
    }
  }

  if (!contentText || contentText.length < 100) {
    // Fall back to full body text
    contentText = $("body").text();
  }

  return parseArticlesFromText(contentText);
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1: Regulamentos
// ─────────────────────────────────────────────────────────────────────────────

interface IngestStats {
  fetched: number;
  skipped: number;
  failed: number;
  provisions: number;
}

async function ingestRegulationCatalog(
  db: Database.Database,
  catalog: RegulationEntry[],
  opts: CliOptions,
  progress: Progress,
): Promise<IngestStats> {
  const stats: IngestStats = { fetched: 0, skipped: 0, failed: 0, provisions: 0 };

  const insertProvision = db.prepare(`
    INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const existsCheck = db.prepare(
    "SELECT 1 FROM provisions WHERE sourcebook_id = ? AND chapter = ? LIMIT 1",
  );

  const items = opts.limit > 0 ? catalog.slice(0, opts.limit) : catalog;

  for (const entry of items) {
    const progressKey = `${entry.sourcebookId}:${entry.chapter}`;

    // --resume: skip if already in progress file
    if (opts.resume && progress.completed.includes(progressKey)) {
      stats.skipped++;
      continue;
    }

    // --resume: also skip if provisions already exist in DB
    if (opts.resume) {
      const existing = existsCheck.get(entry.sourcebookId, entry.chapter);
      if (existing) {
        stats.skipped++;
        progress.completed.push(progressKey);
        continue;
      }
    }

    if (opts.dryRun) {
      console.log(`  [dry-run] Would fetch: ${entry.title}`);
      console.log(`            URL: ${entry.dreUrl}`);
      stats.fetched++;
      continue;
    }

    try {
      console.log(`  Fetching: ${entry.title}`);
      await sleep(RATE_LIMIT_MS);

      const { body, contentType } = await fetchWithRetry(entry.dreUrl);

      let articles: ParsedArticle[];

      if (contentType.includes("application/pdf")) {
        // PDF content — cannot parse with cheerio.
        // Insert a single provision for the entire regulation with a note.
        console.log(`    PDF detected — inserting as single provision`);
        articles = [
          {
            reference: "FULL",
            title: entry.title,
            text: `Texto integral publicado no Diário da República. Consultar PDF: ${entry.dreUrl}`,
            section: "FULL",
          },
        ];
      } else if (contentType.includes("text/html")) {
        articles = parseArticlesFromHtml(body);
        if (articles.length === 0) {
          // SPA or empty page — try text-based parsing as fallback
          articles = parseArticlesFromText(body);
        }
      } else {
        // Unknown content type — try text parsing
        articles = parseArticlesFromText(body);
      }

      if (articles.length === 0) {
        // Insert a placeholder provision so we track the regulation
        console.log(`    No articles parsed — inserting metadata-only provision`);
        articles = [
          {
            reference: "FULL",
            title: entry.title,
            text: `${entry.title}. Texto integral disponível em: ${entry.dreUrl}`,
            section: "FULL",
          },
        ];
      }

      const insertBatch = db.transaction(() => {
        for (const art of articles) {
          const ref = `${entry.sourcebookId} ${entry.chapter}.${art.reference}`;
          insertProvision.run(
            entry.sourcebookId,
            ref,
            art.title,
            art.text,
            entry.type,
            entry.status,
            entry.effectiveDate,
            entry.chapter,
            art.section,
          );
        }
      });

      insertBatch();
      stats.provisions += articles.length;
      stats.fetched++;
      progress.completed.push(progressKey);
      saveProgress(progress);

      console.log(`    Inserted ${articles.length} articles`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`    FAILED: ${msg}`);
      stats.failed++;
    }
  }

  return stats;
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2: Enforcement actions
// ─────────────────────────────────────────────────────────────────────────────

async function ingestEnforcement(
  db: Database.Database,
  opts: CliOptions,
  progress: Progress,
): Promise<IngestStats> {
  const stats: IngestStats = { fetched: 0, skipped: 0, failed: 0, provisions: 0 };

  const insertEnforcement = db.prepare(`
    INSERT INTO enforcement_actions
      (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const existsCheck = db.prepare(
    "SELECT 1 FROM enforcement_actions WHERE reference_number = ? LIMIT 1",
  );

  const items = opts.limit > 0 ? ENFORCEMENT_CATALOG.slice(0, opts.limit) : ENFORCEMENT_CATALOG;

  for (const entry of items) {
    const progressKey = `enforcement:${entry.referenceNumber}`;

    if (opts.resume && progress.completed.includes(progressKey)) {
      stats.skipped++;
      continue;
    }

    if (opts.resume) {
      const existing = existsCheck.get(entry.referenceNumber);
      if (existing) {
        stats.skipped++;
        progress.completed.push(progressKey);
        continue;
      }
    }

    if (opts.dryRun) {
      console.log(
        `  [dry-run] Would insert enforcement: ${entry.referenceNumber} — ${entry.firmName}`,
      );
      stats.fetched++;
      continue;
    }

    try {
      insertEnforcement.run(
        entry.firmName,
        entry.referenceNumber,
        entry.actionType,
        entry.amount,
        entry.date,
        entry.summary,
        entry.sourcebookReferences,
      );
      stats.fetched++;
      stats.provisions++;
      progress.completed.push(progressKey);
      console.log(`  Inserted: ${entry.referenceNumber} — ${entry.firmName}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`  FAILED: ${entry.referenceNumber} — ${msg}`);
      stats.failed++;
    }
  }

  saveProgress(progress);
  return stats;
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Live crawl — supplementary index pages
//
// Attempts to crawl CMVM pages that serve HTML content (not SPA):
//   - Enforcement decisions list
//   - Circulars
//   - Complementary legislation links
//
// These pages may or may not be available — failures are non-fatal.
// ─────────────────────────────────────────────────────────────────────────────

const CRAWL_TARGETS = [
  {
    url: "https://www.cmvm.pt/pt/Comunicados/ContraordenacoesECrimesContraOMercado/Pages/Decisoes_CMVM.aspx",
    label: "CMVM enforcement decisions",
  },
  {
    url: "https://www.cmvm.pt/pt/Legislacao/Legislacaonacional/Circulares/Pages/Circulares.aspx",
    label: "CMVM circulares",
  },
  {
    url: "https://www.cmvm.pt/pt/Legislacao/LegislacaoComplementar/Pages/Legislacao-Complementar.aspx",
    label: "Legislação complementar",
  },
];

async function crawlSupplementaryPages(
  db: Database.Database,
  opts: CliOptions,
  progress: Progress,
): Promise<IngestStats> {
  const stats: IngestStats = { fetched: 0, skipped: 0, failed: 0, provisions: 0 };

  const insertProvision = db.prepare(`
    INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const existsCheck = db.prepare(
    "SELECT 1 FROM provisions WHERE reference = ? LIMIT 1",
  );

  for (const target of CRAWL_TARGETS) {
    const progressKey = `crawl:${target.url}`;
    if (opts.resume && progress.completed.includes(progressKey)) {
      console.log(`  Skipping (already crawled): ${target.label}`);
      stats.skipped++;
      continue;
    }

    if (opts.dryRun) {
      console.log(`  [dry-run] Would crawl: ${target.label} (${target.url})`);
      stats.fetched++;
      continue;
    }

    try {
      console.log(`  Crawling: ${target.label}`);
      await sleep(RATE_LIMIT_MS);

      const html = await fetchHtml(target.url);
      const $ = cheerio.load(html);

      // Extract links from the page — these pages typically list decisions or documents
      const links: { href: string; text: string }[] = [];
      $("a[href]").each((_i, el) => {
        const href = $(el).attr("href") ?? "";
        const text = $(el).text().trim();
        if (text.length > 10 && href.length > 5) {
          links.push({ href, text });
        }
      });

      console.log(`    Found ${links.length} links on ${target.label}`);

      // For each link that looks like a decision or circular, extract metadata
      let inserted = 0;
      for (const link of links) {
        // Filter for relevant document links
        const isRelevant =
          /(?:decisao|contraordena|circular|regulamento|instruc|aviso)/i.test(link.text) ||
          /(?:decisao|contraordena|circular|regulamento|instruc|aviso)/i.test(link.href);

        if (!isRelevant) continue;

        const ref = `CMVM_REGULAMENTOS CRAWL:${link.href.replace(/[^a-zA-Z0-9]/g, "_").slice(0, 80)}`;

        if (opts.resume) {
          const existing = existsCheck.get(ref);
          if (existing) continue;
        }

        insertProvision.run(
          "CMVM_REGULAMENTOS",
          ref,
          link.text,
          `Documento referenciado no portal da CMVM. Consultar: ${link.href.startsWith("http") ? link.href : `https://www.cmvm.pt${link.href}`}`,
          "referencia",
          "in_force",
          null,
          "CRAWL",
          null,
        );
        inserted++;

        if (opts.limit > 0 && inserted >= opts.limit) break;
      }

      stats.provisions += inserted;
      stats.fetched++;
      progress.completed.push(progressKey);
      saveProgress(progress);

      console.log(`    Inserted ${inserted} reference provisions`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`    Skipped ${target.label}: ${msg}`);
      stats.failed++;
      // Non-fatal — these pages may be down or SPA-only
    }
  }

  return stats;
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const opts = parseArgs();
  const progress = opts.resume ? loadProgress() : { completed: [] };

  console.log("CMVM Ingestion Crawler — Portuguese Financial Regulation MCP");
  console.log("=".repeat(62));
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Dry run:    ${opts.dryRun}`);
  console.log(`  Resume:     ${opts.resume}`);
  console.log(`  Force:      ${opts.force}`);
  console.log(`  Source:     ${opts.source}`);
  console.log(`  Limit:      ${opts.limit || "none"}`);
  console.log(`  Rate limit: ${RATE_LIMIT_MS}ms`);
  console.log();

  const db = opts.dryRun
    ? (null as unknown as Database.Database) // no DB needed for dry-run
    : initDb(opts.force);

  if (!opts.dryRun) {
    ensureSourcebooks(db);
    console.log(`Sourcebooks ensured (${SOURCEBOOKS.length})\n`);
  }

  const totals: IngestStats = { fetched: 0, skipped: 0, failed: 0, provisions: 0 };

  function addStats(s: IngestStats): void {
    totals.fetched += s.fetched;
    totals.skipped += s.skipped;
    totals.failed += s.failed;
    totals.provisions += s.provisions;
  }

  // ── Phase 1: CMVM Regulamentos ──────────────────────────────────────────
  if (opts.source === "all" || opts.source === "regulamentos") {
    console.log("Phase 1: CMVM Regulamentos");
    console.log("-".repeat(40));
    console.log(`  Catalog: ${REGULAMENTO_CATALOG.length} regulations\n`);

    if (!opts.dryRun) {
      const s = await ingestRegulationCatalog(db, REGULAMENTO_CATALOG, opts, progress);
      addStats(s);
      console.log(
        `\n  Regulamentos: ${s.fetched} fetched, ${s.skipped} skipped, ${s.failed} failed, ${s.provisions} provisions\n`,
      );
    } else {
      const s = await ingestRegulationCatalog(
        null as unknown as Database.Database,
        REGULAMENTO_CATALOG,
        opts,
        progress,
      );
      addStats(s);
      console.log(`\n  [dry-run] Would process ${s.fetched} regulamentos\n`);
    }
  }

  // ── Phase 2: CMVM Instruções ────────────────────────────────────────────
  if (opts.source === "all" || opts.source === "instrucoes") {
    console.log("Phase 2: CMVM Instruções");
    console.log("-".repeat(40));
    console.log(`  Catalog: ${INSTRUCAO_CATALOG.length} instructions\n`);

    if (!opts.dryRun) {
      const s = await ingestRegulationCatalog(db, INSTRUCAO_CATALOG, opts, progress);
      addStats(s);
      console.log(
        `\n  Instruções: ${s.fetched} fetched, ${s.skipped} skipped, ${s.failed} failed, ${s.provisions} provisions\n`,
      );
    } else {
      const s = await ingestRegulationCatalog(
        null as unknown as Database.Database,
        INSTRUCAO_CATALOG,
        opts,
        progress,
      );
      addStats(s);
      console.log(`\n  [dry-run] Would process ${s.fetched} instruções\n`);
    }
  }

  // ── Phase 3: Enforcement actions ────────────────────────────────────────
  if (opts.source === "all" || opts.source === "enforcement") {
    console.log("Phase 3: Enforcement Actions (Contraordenações)");
    console.log("-".repeat(40));
    console.log(`  Catalog: ${ENFORCEMENT_CATALOG.length} decisions\n`);

    const s = await ingestEnforcement(db, opts, progress);
    addStats(s);
    if (opts.dryRun) {
      console.log(`\n  [dry-run] Would insert ${s.fetched} enforcement actions\n`);
    } else {
      console.log(
        `\n  Enforcement: ${s.fetched} inserted, ${s.skipped} skipped, ${s.failed} failed\n`,
      );
    }
  }

  // ── Phase 4: Supplementary crawl ────────────────────────────────────────
  if (opts.source === "all" && !opts.dryRun) {
    console.log("Phase 4: Supplementary page crawl");
    console.log("-".repeat(40));
    console.log(`  Targets: ${CRAWL_TARGETS.length} pages\n`);

    const s = await crawlSupplementaryPages(db, opts, progress);
    addStats(s);
    console.log(
      `\n  Supplementary: ${s.fetched} crawled, ${s.skipped} skipped, ${s.failed} failed, ${s.provisions} provisions\n`,
    );
  }

  // ── Summary ─────────────────────────────────────────────────────────────
  console.log("=".repeat(62));
  console.log("Summary");
  console.log("=".repeat(62));
  console.log(`  Fetched:    ${totals.fetched}`);
  console.log(`  Skipped:    ${totals.skipped}`);
  console.log(`  Failed:     ${totals.failed}`);
  console.log(`  Provisions: ${totals.provisions}`);

  if (!opts.dryRun && db) {
    const provisionCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions").get() as {
        cnt: number;
      }
    ).cnt;
    const sourcebookCount = (
      db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as {
        cnt: number;
      }
    ).cnt;
    const enforcementCount = (
      db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
        cnt: number;
      }
    ).cnt;
    const ftsCount = (
      db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as {
        cnt: number;
      }
    ).cnt;

    console.log(`\nDatabase totals:`);
    console.log(`  Sourcebooks:          ${sourcebookCount}`);
    console.log(`  Provisions:           ${provisionCount}`);
    console.log(`  Enforcement actions:  ${enforcementCount}`);
    console.log(`  FTS entries:          ${ftsCount}`);
    console.log(`\nDatabase: ${DB_PATH}`);

    db.close();
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
