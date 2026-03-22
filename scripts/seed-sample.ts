/**
 * Seed the Portuguese financial regulation database with sample provisions.
 *
 * Inserts representative provisions from CMVM_Regulamentos, CMVM_Instrucoes,
 * BDP_Avisos (Banco de Portugal), and ASF_Normas so MCP tools can be tested
 * without a full ingestion run.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CMVM_DB_PATH"] ?? "data/cmvm.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
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

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  // ── CMVM Regulamentos ───────────────────────────────────────────────────
  {
    sourcebook_id: "CMVM_REGULAMENTOS",
    reference: "CMVM_REGULAMENTOS REG.2007.02.A3",
    title: "Artigo 3.º — Categorias de clientes",
    text: "Os intermediários financeiros devem classificar os seus clientes em contrapartes elegíveis, clientes profissionais e clientes não profissionais. A classificação deve ser efectuada antes da prestação de qualquer serviço de investimento e determina o nível de protecção aplicável ao cliente.",
    type: "regulamento",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "REG.2007.02",
    section: "A3",
  },
  {
    sourcebook_id: "CMVM_REGULAMENTOS",
    reference: "CMVM_REGULAMENTOS REG.2007.02.A12",
    title: "Artigo 12.º — Adequação e avaliação de conveniência",
    text: "O intermediário financeiro deve obter informação necessária sobre o conhecimento e experiência do cliente em matéria de investimento, a sua situação financeira e os seus objectivos de investimento, de modo a poder recomendar-lhe os instrumentos financeiros e os serviços de investimento adequados ou convenientes. O intermediário não pode recomendar instrumentos ou serviços inadequados ao perfil do cliente.",
    type: "regulamento",
    status: "in_force",
    effective_date: "2007-11-01",
    chapter: "REG.2007.02",
    section: "A12",
  },
  {
    sourcebook_id: "CMVM_REGULAMENTOS",
    reference: "CMVM_REGULAMENTOS REG.2012.04.A5",
    title: "Artigo 5.º — Política de gestão de conflitos de interesses",
    text: "Os intermediários financeiros devem estabelecer e manter uma política de gestão de conflitos de interesses eficaz. A política deve identificar as circunstâncias susceptíveis de gerar conflitos de interesses e estabelecer os procedimentos a seguir e as medidas a adoptar para gerir esses conflitos.",
    type: "regulamento",
    status: "in_force",
    effective_date: "2012-07-01",
    chapter: "REG.2012.04",
    section: "A5",
  },

  // ── CMVM Instruções ─────────────────────────────────────────────────────
  {
    sourcebook_id: "CMVM_INSTRUCOES",
    reference: "CMVM_INSTRUCOES INSTR.2021.01.S3",
    title: "Secção 3 — Requisitos de informação pré-contratual",
    text: "Antes da celebração de um contrato de intermediação financeira, o intermediário deve disponibilizar ao cliente, em suporte duradouro, a informação relativa à sua política de execução de ordens, ao custo total da prestação do serviço, e aos riscos associados aos instrumentos financeiros em causa. A informação deve ser disponibilizada com antecedência suficiente para que o cliente possa tomar uma decisão informada.",
    type: "instrucao",
    status: "in_force",
    effective_date: "2021-02-01",
    chapter: "INSTR.2021.01",
    section: "S3",
  },

  // ── Banco de Portugal Avisos ─────────────────────────────────────────────
  {
    sourcebook_id: "BDP_AVISOS",
    reference: "BDP_AVISOS AV.2021.01.A4",
    title: "Artigo 4.º — Requisitos de capital interno (ICAAP)",
    text: "As instituições de crédito devem dispor de estratégias e processos sólidos, eficazes e exaustivos para avaliar e manter de forma permanente os montantes, tipos e distribuição do capital interno que consideram adequados para cobrir a natureza e o nível de riscos a que estão ou possam vir a estar expostas. Estes processos são sujeitos a revisão interna regular para garantir que permanecem exaustivos e proporcionados à natureza, escala e complexidade das actividades da instituição.",
    type: "aviso",
    status: "in_force",
    effective_date: "2021-06-01",
    chapter: "AV.2021.01",
    section: "A4",
  },
  {
    sourcebook_id: "BDP_AVISOS",
    reference: "BDP_AVISOS AV.2021.01.A7",
    title: "Artigo 7.º — Governo interno",
    text: "As instituições de crédito devem dispor de sólidos mecanismos de governo interno, incluindo uma estrutura organizativa clara com linhas de responsabilidade bem definidas, transparentes e coerentes, processos eficazes de identificação, gestão, acompanhamento e comunicação dos riscos a que estão ou possam vir a estar expostas, e mecanismos adequados de controlo interno, incluindo procedimentos administrativos e contabilísticos sólidos.",
    type: "aviso",
    status: "in_force",
    effective_date: "2021-06-01",
    chapter: "AV.2021.01",
    section: "A7",
  },
  {
    sourcebook_id: "BDP_AVISOS",
    reference: "BDP_AVISOS AV.2022.03.A2",
    title: "Artigo 2.º — Requisitos em matéria de branqueamento de capitais",
    text: "As instituições de crédito são obrigadas a implementar políticas e procedimentos internos adequados em matéria de prevenção do branqueamento de capitais e do financiamento do terrorismo. As políticas devem incluir medidas de diligência devida relativamente à clientela, comunicação de operações suspeitas, conservação de registos e formação do pessoal.",
    type: "aviso",
    status: "in_force",
    effective_date: "2022-01-01",
    chapter: "AV.2022.03",
    section: "A2",
  },

  // ── ASF Normas Regulamentares ─────────────────────────────────────────────
  {
    sourcebook_id: "ASF_NORMAS",
    reference: "ASF_NORMAS NR.2016.07.A3",
    title: "Artigo 3.º — Sistema de governação das empresas de seguros",
    text: "As empresas de seguros e de resseguros devem dispor de um sistema de governação eficaz que garanta uma gestão sã e prudente da actividade. O sistema de governação deve incluir uma estrutura organizativa transparente com uma atribuição clara e uma adequada separação de funções, mecanismos eficazes de transmissão de informações e uma política de prevenção de conflitos de interesses.",
    type: "norma",
    status: "in_force",
    effective_date: "2016-12-01",
    chapter: "NR.2016.07",
    section: "A3",
  },
  {
    sourcebook_id: "ASF_NORMAS",
    reference: "ASF_NORMAS NR.2016.07.A9",
    title: "Artigo 9.º — Gestão de riscos",
    text: "As empresas de seguros devem dispor de um sistema eficaz de gestão de riscos que abranja as estratégias, processos e procedimentos de comunicação de informações necessários para identificar, avaliar, acompanhar, gerir e comunicar de forma contínua os riscos a que estão ou possam vir a estar expostas, individualmente e de forma agregada.",
    type: "norma",
    status: "in_force",
    effective_date: "2016-12-01",
    chapter: "NR.2016.07",
    section: "A9",
  },
  {
    sourcebook_id: "ASF_NORMAS",
    reference: "ASF_NORMAS NR.2022.01.A5",
    title: "Artigo 5.º — Requisitos de divulgação de informação sobre sustentabilidade",
    text: "As empresas de seguros que comercializam produtos de investimento com base em seguros devem divulgar nos documentos de informação pré-contratual e nos relatórios periódicos de que forma são integrados os riscos de sustentabilidade nas decisões de investimento e de que modo os principais impactos negativos nos factores de sustentabilidade são considerados.",
    type: "norma",
    status: "in_force",
    effective_date: "2022-03-10",
    chapter: "NR.2022.01",
    section: "A5",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id,
      p.reference,
      p.title,
      p.text,
      p.type,
      p.status,
      p.effective_date,
      p.chapter,
      p.section,
    );
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "EuroBic — Banco Europeu de Investimento e Comércio, S.A.",
    reference_number: "CMVM-ENF-2021-001",
    action_type: "fine",
    amount: 2_100_000,
    date: "2021-09-15",
    summary:
      "A CMVM aplicou uma coima de 2,1 milhões de euros ao EuroBic por violações dos deveres de prevenção do branqueamento de capitais. A instituição não implementou adequadamente os procedimentos de diligência devida relativamente à clientela nem acompanhou devidamente as operações de alto risco relacionadas com a conta da Sonangol.",
    sourcebook_references: "CMVM_REGULAMENTOS REG.2012.04.A5, BDP_AVISOS AV.2022.03.A2",
  },
  {
    firm_name: "Caixa Económica Montepio Geral",
    reference_number: "BDP-ENF-2022-002",
    action_type: "restriction",
    amount: 0,
    date: "2022-03-01",
    summary:
      "O Banco de Portugal determinou a aplicação de medidas correctivas ao Montepio Geral na sequência de deficiências identificadas no sistema de controlo interno e no processo de avaliação do capital interno (ICAAP). A instituição foi obrigada a reforçar o capital próprio e a apresentar um plano de saneamento.",
    sourcebook_references: "BDP_AVISOS AV.2021.01.A4, BDP_AVISOS AV.2021.01.A7",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name,
      e.reference_number,
      e.action_type,
      e.amount,
      e.date,
      e.summary,
      e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

const provisionCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }
).cnt;
const sourcebookCount = (
  db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }
).cnt;
const enforcementCount = (
  db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }
).cnt;
const ftsCount = (
  db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }
).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
