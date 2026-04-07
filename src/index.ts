#!/usr/bin/env node

/**
 * Portuguese Financial Regulation MCP — stdio entry point.
 *
 * Provides MCP tools for querying CMVM regulamentos e instruções,
 * Banco de Portugal avisos, and ASF normas regulamentares.
 *
 * Tool prefix: pt_fin_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  listSourcebooks,
  searchProvisions,
  getProvision,
  searchEnforcement,
  checkProvisionCurrency,
} from "./db.js";
import { buildCitation } from "./citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "portuguese-financial-regulation-mcp";

const TOOLS = [
  {
    name: "pt_fin_search_regulations",
    description:
      "Pesquisa de texto integral em regulamentos e instruções da CMVM, avisos do Banco de Portugal e normas regulamentares da ASF. (Full-text search across CMVM regulations, Banco de Portugal notices, and ASF regulatory norms.)",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Termo de pesquisa (ex.: 'requisitos prudenciais', 'branqueamento de capitais', 'governo societário'). Query in Portuguese or English.",
        },
        sourcebook: {
          type: "string",
          description: "Filtrar por sourcebook (ex.: CMVM_REGULAMENTOS, CMVM_INSTRUCOES, BDP_AVISOS, ASF_NORMAS). Optional.",
        },
        status: {
          type: "string",
          enum: ["in_force", "deleted", "not_yet_in_force"],
          description: "Filtrar por estado da disposição. Defaults to all statuses.",
        },
        limit: {
          type: "number",
          description: "Número máximo de resultados. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "pt_fin_get_regulation",
    description:
      "Obtém uma disposição específica pelo sourcebook e referência. Accepts references like 'CMVM_REGULAMENTOS REG.2007.02.A3' or 'BDP_AVISOS AV.2021.01.A1'.",
    inputSchema: {
      type: "object" as const,
      properties: {
        sourcebook: {
          type: "string",
          description: "Identificador do sourcebook (ex.: CMVM_REGULAMENTOS, BDP_AVISOS, ASF_NORMAS)",
        },
        reference: {
          type: "string",
          description: "Referência completa da disposição (ex.: 'CMVM_REGULAMENTOS REG.2007.02.A3')",
        },
      },
      required: ["sourcebook", "reference"],
    },
  },
  {
    name: "pt_fin_list_sourcebooks",
    description:
      "Lista todos os sourcebooks disponíveis com nomes e descrições. (List all available sourcebooks with names and descriptions.)",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "pt_fin_search_enforcement",
    description:
      "Pesquisa de decisões de supervisão — coimas, suspensões de actividade e advertências públicas da CMVM, Banco de Portugal e ASF. (Search enforcement decisions — fines, activity suspensions, and public warnings from CMVM, Banco de Portugal, and ASF.)",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Termo de pesquisa (nome da entidade, tipo de infracção, etc.)",
        },
        action_type: {
          type: "string",
          enum: ["fine", "ban", "restriction", "warning"],
          description: "Filtrar por tipo de medida. Optional.",
        },
        limit: {
          type: "number",
          description: "Número máximo de resultados. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "pt_fin_check_currency",
    description:
      "Verifica se uma referência específica está actualmente em vigor. (Check whether a specific provision reference is currently in force.)",
    inputSchema: {
      type: "object" as const,
      properties: {
        reference: {
          type: "string",
          description: "Referência completa da disposição a verificar",
        },
      },
      required: ["reference"],
    },
  },
  {
    name: "pt_fin_about",
    description: "Return metadata about this MCP server: version, data source, tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

const SearchRegulationsArgs = z.object({
  query: z.string().min(1),
  sourcebook: z.string().optional(),
  status: z.enum(["in_force", "deleted", "not_yet_in_force"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetRegulationArgs = z.object({
  sourcebook: z.string().min(1),
  reference: z.string().min(1),
});

const SearchEnforcementArgs = z.object({
  query: z.string().min(1),
  action_type: z.enum(["fine", "ban", "restriction", "warning"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const CheckCurrencyArgs = z.object({
  reference: z.string().min(1),
});

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "pt_fin_search_regulations": {
        const parsed = SearchRegulationsArgs.parse(args);
        const results = searchProvisions({
          query: parsed.query,
          sourcebook: parsed.sourcebook,
          status: parsed.status,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "pt_fin_get_regulation": {
        const parsed = GetRegulationArgs.parse(args);
        const provision = getProvision(parsed.sourcebook, parsed.reference);
        if (!provision) {
          return errorContent(
            `Disposição não encontrada: ${parsed.sourcebook} ${parsed.reference}`,
          );
        }
        const p = provision as Record<string, unknown>;
        return textContent({
          ...p,
          _citation: buildCitation(
            String(p.reference ?? parsed.reference),
            String(p.title ?? p.reference ?? parsed.reference),
            "pt_fin_get_regulation",
            { sourcebook: parsed.sourcebook, reference: parsed.reference },
          ),
        });
      }

      case "pt_fin_list_sourcebooks": {
        const sourcebooks = listSourcebooks();
        return textContent({ sourcebooks, count: sourcebooks.length });
      }

      case "pt_fin_search_enforcement": {
        const parsed = SearchEnforcementArgs.parse(args);
        const results = searchEnforcement({
          query: parsed.query,
          action_type: parsed.action_type,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "pt_fin_check_currency": {
        const parsed = CheckCurrencyArgs.parse(args);
        const currency = checkProvisionCurrency(parsed.reference);
        return textContent(currency);
      }

      case "pt_fin_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "Portuguese Financial Regulation MCP server. Provides access to CMVM regulations and instructions, Banco de Portugal notices (avisos), and ASF regulatory norms (normas regulamentares).",
          data_source: "CMVM (https://www.cmvm.pt/), Banco de Portugal (https://www.bportugal.pt/), ASF (https://www.asf.com.pt/)",
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
