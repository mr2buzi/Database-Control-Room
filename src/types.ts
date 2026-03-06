export type ColumnType = "INT" | "TEXT";

export type Column = {
  name: string;
  type: ColumnType;
  indexed?: boolean;
};

export type Table = {
  name: string;
  rowCount: number;
  columns: Column[];
};

export type QueryRun = {
  id: number;
  timestamp: string;
  summary: string;
  query: string;
  strategy: string;
  latencyMs: number;
  rowsRead: number;
  rowsReturned: number;
  status: "ok" | "warning";
  source: "engine" | "mock";
  statementCount: number;
};

export type QueryResult = {
  label: string;
  status: "ok" | "warning";
  strategy: string;
  latencyMs: number;
  rowsRead: number;
  rowsReturned: number;
  columns: string[];
  rows: Array<Record<string, string | number>>;
  notes: string[];
  source: "engine" | "mock";
  planKind: string;
  message?: string;
  astKind: string;
  statementCount: number;
  rawAst: string;
  rawPlan: string;
  filterLabel?: string;
  limit?: number | null;
  usedIndexName?: string | null;
};

export type InsightCard = {
  title: string;
  value: string;
  tone: "good" | "neutral" | "warn";
  detail: string;
};

export type BridgePlan = {
  kind: string;
  table: string | null;
  projection: string[];
  filter: {
    column: string;
    op: string;
    value: string | number;
  } | null;
  limit: number | null;
  used_index: string | null;
  description: string;
};

export type BridgeQueryResponse = {
  ast: Record<string, unknown>;
  plan: BridgePlan;
  columns: string[];
  rows: Array<Array<string | number>>;
  stats: {
    latency_ms: number;
    rows_read: number;
    rows_returned: number;
    used_index: boolean;
  };
  error: { message: string } | null;
  message?: string;
};

export type BridgeQueryBatchResponse = {
  statements: BridgeQueryResponse[];
  final: BridgeQueryResponse;
  error: { message: string } | null;
};

export type BridgeRuntimeInfo = {
  mode: string;
  dataPath: string;
};

export type BridgeSchemaResponse = {
  tables: Table[];
  error: { message: string } | null;
};
