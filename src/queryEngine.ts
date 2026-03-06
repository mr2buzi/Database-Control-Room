import { defaultQuery, sampleResults } from "./data";
import type {
  BridgeQueryBatchResponse,
  BridgeQueryResponse,
  BridgeRuntimeInfo,
  BridgeSchemaResponse,
  QueryResult,
  QueryRun,
  Table
} from "./types";

const nowLabel = () =>
  new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).format(new Date());

export const hasDesktopBridge = () =>
  typeof window !== "undefined" && typeof window.slatedbBridge !== "undefined";

const hasBridgeMethod = <T extends keyof NonNullable<Window["slatedbBridge"]>>(method: T) =>
  hasDesktopBridge() && typeof window.slatedbBridge?.[method] === "function";

export const getRuntimeInfo = async (): Promise<BridgeRuntimeInfo | null> => {
  if (!hasBridgeMethod("getRuntimeInfo")) {
    return null;
  }
  return window.slatedbBridge!.getRuntimeInfo();
};

export const getSchema = async (): Promise<Table[] | null> => {
  if (!hasBridgeMethod("getSchema")) {
    return null;
  }
  const payload: BridgeSchemaResponse = await window.slatedbBridge!.getSchema();
  return payload.tables;
};

export const runQuery = async (
  query: string,
  nextId: number
): Promise<{ result: QueryResult; history: QueryRun }> => {
  if (!hasBridgeMethod("runQuery")) {
    return Promise.resolve(runMockQuery(query, nextId));
  }

  const payload = await window.slatedbBridge!.runQuery(query);
  return transformEngineResponse(query, nextId, payload);
};

const runMockQuery = (
  query: string,
  nextId: number
): { result: QueryResult; history: QueryRun } => {
  const normalized = query.trim().replace(/\s+/g, " ").toLowerCase();

  let result = sampleResults.default;
  let summary = "Browser fallback on users";

  if (normalized.includes("from orders")) {
    result = sampleResults.orders;
    summary = "Browser fallback on orders";
  } else if (normalized.includes("from audit_log")) {
    result = sampleResults.scan;
    summary = "Browser fallback scan";
  } else if (normalized.startsWith("explain")) {
    result = sampleResults.explain;
    summary = "Browser fallback explain";
  } else if (normalized === defaultQuery.trim().replace(/\s+/g, " ").toLowerCase()) {
    summary = "Browser fallback default query";
  }

  return {
    result,
    history: {
      id: nextId,
      timestamp: nowLabel(),
      summary,
      query,
      strategy: result.strategy,
      latencyMs: result.latencyMs,
      rowsRead: result.rowsRead,
      rowsReturned: result.rowsReturned,
      status: result.status,
      source: "mock",
      statementCount: result.statementCount
    }
  };
};

const transformEngineResponse = (
  query: string,
  nextId: number,
  payload: BridgeQueryResponse | BridgeQueryBatchResponse
): { result: QueryResult; history: QueryRun } => {
  const normalized = normalizePayload(payload);
  const payloadForResult = normalized.final;
  const rowsFromFinal = payloadForResult.rows.map((row) =>
    Object.fromEntries(
      payloadForResult.columns.map((column, index) => [column, row[index] ?? ""])
    )
  );
  const usedIndex =
    Boolean(payloadForResult.plan.used_index) || payloadForResult.stats.used_index;
  const status =
    usedIndex ||
    payloadForResult.plan.kind === "Ddl" ||
    payloadForResult.plan.kind === "Transaction"
      ? "ok"
      : payloadForResult.stats.rows_read > Math.max(1, payloadForResult.stats.rows_returned)
        ? "warning"
        : "ok";
  const astKind = String(payloadForResult.ast.kind ?? "Unknown");
  const filterLabel = payloadForResult.plan.filter
    ? `${payloadForResult.plan.filter.column} = ${String(payloadForResult.plan.filter.value)}`
    : undefined;

  const result: QueryResult = {
    label: payloadForResult.plan.table ?? (payloadForResult.columns[0] ?? "result"),
    status,
    strategy: payloadForResult.plan.description,
    latencyMs: payloadForResult.stats.latency_ms,
    rowsRead: payloadForResult.stats.rows_read,
    rowsReturned: payloadForResult.stats.rows_returned,
    columns: payloadForResult.columns,
    rows: rowsFromFinal,
    notes: buildNotes(payloadForResult, usedIndex, normalized.statementCount),
    source: "engine",
    planKind: payloadForResult.plan.kind,
    message: payloadForResult.message,
    astKind,
    statementCount: normalized.statementCount,
    rawAst: JSON.stringify(payloadForResult.ast, null, 2),
    rawPlan: JSON.stringify(payloadForResult.plan, null, 2),
    filterLabel,
    limit: payloadForResult.plan.limit,
    usedIndexName: payloadForResult.plan.used_index
  };

  return {
    result,
    history: {
      id: nextId,
      timestamp: nowLabel(),
      summary: buildSummary(payloadForResult, usedIndex, normalized.statementCount),
      query,
      strategy: result.strategy,
      latencyMs: result.latencyMs,
      rowsRead: result.rowsRead,
      rowsReturned: result.rowsReturned,
      status: result.status,
      source: "engine",
      statementCount: normalized.statementCount
    }
  };
};

const buildSummary = (
  payload: BridgeQueryResponse,
  usedIndex: boolean,
  statementCount: number
) => {
  const kind = String(payload.ast.kind ?? payload.plan.kind);
  const table = payload.plan.table ?? "catalog";
  const prefix = statementCount > 1 ? `${statementCount} statements, final ` : "";
  if (usedIndex) {
    return `${prefix}${kind} via index on ${table}`;
  }
  return `${prefix}${kind} on ${table}`;
};

const buildNotes = (
  payload: BridgeQueryResponse,
  usedIndex: boolean,
  statementCount: number
) => {
  const astKind = String(payload.ast.kind ?? "Unknown");
  const notes = [
    `AST node: ${astKind}`,
    `Planner path: ${payload.plan.kind}`,
    `Statement count: ${statementCount}`
  ];

  if (payload.plan.used_index) {
    notes.push(`Index used: ${payload.plan.used_index}`);
  } else if (usedIndex) {
    notes.push("The engine reported indexed access.");
  } else {
    notes.push("No index was selected for this statement.");
  }

  if (payload.message) {
    notes.push(`Engine message: ${payload.message}`);
  }

  return notes;
};

const normalizePayload = (
  payload: BridgeQueryResponse | BridgeQueryBatchResponse
): { final: BridgeQueryResponse; statementCount: number } => {
  if ("final" in payload && Array.isArray(payload.statements)) {
    return {
      final: payload.final,
      statementCount: payload.statements.length
    };
  }
  return {
    final: payload as BridgeQueryResponse,
    statementCount: 1
  };
};
