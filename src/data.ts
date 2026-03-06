import type { InsightCard, QueryResult, QueryRun, Table } from "./types";

export const fallbackSchema: Table[] = [
  {
    name: "users",
    rowCount: 4,
    columns: [
      { name: "id", type: "INT", indexed: true },
      { name: "name", type: "TEXT" },
      { name: "tier", type: "TEXT" }
    ]
  },
  {
    name: "orders",
    rowCount: 3,
    columns: [
      { name: "id", type: "INT", indexed: true },
      { name: "user_id", type: "INT", indexed: true },
      { name: "amount_cents", type: "INT" },
      { name: "status", type: "TEXT" }
    ]
  },
  {
    name: "audit_log",
    rowCount: 3,
    columns: [
      { name: "id", type: "INT", indexed: true },
      { name: "entity", type: "TEXT" },
      { name: "action", type: "TEXT", indexed: true },
      { name: "created_at", type: "TEXT" }
    ]
  }
];

export const defaultQuery = `SELECT id, name, tier
FROM users
WHERE id = 2
LIMIT 1;`;

export const insights: InsightCard[] = [
  {
    title: "Desktop Loop",
    value: "IPC",
    tone: "good",
    detail: "The workbench talks to the Rust CLI through Electron, not HTTP."
  },
  {
    title: "Planner Confidence",
    value: "High",
    tone: "good",
    detail: "Equality and range predicates line up with the seeded demo indexes."
  },
  {
    title: "Storage Style",
    value: "Paged",
    tone: "neutral",
    detail: "Results come from the real heap-table and WAL-backed engine."
  }
];

export const initialHistory: QueryRun[] = [
  {
    id: 3,
    timestamp: "09:31",
    summary: "Desktop bridge ready",
    query: "EXPLAIN SELECT id, name, tier FROM users WHERE id = 2;",
    strategy: "Awaiting first live engine query",
    latencyMs: 0,
    rowsRead: 0,
    rowsReturned: 0,
    status: "ok",
    source: "mock",
    statementCount: 1
  }
];

export const sampleResults: Record<string, QueryResult> = {
  default: {
    label: "users",
    status: "ok",
    strategy: "Mock index lookup on users.id",
    latencyMs: 3.8,
    rowsRead: 1,
    rowsReturned: 1,
    columns: ["id", "name", "tier"],
    rows: [{ id: 2, name: "Jay", tier: "pro" }],
    notes: [
      "Browser fallback is active.",
      "Launch through Electron to hit the Rust engine."
    ],
    source: "mock",
    planKind: "SeqScan",
    message: "Mock response",
    astKind: "Select",
    statementCount: 1,
    rawAst: "{ kind: Select, table_name: users, limit: 1 }",
    rawPlan: "{ kind: SeqScan, description: mock index lookup on users.id }",
    filterLabel: "id = 2",
    limit: 1,
    usedIndexName: null
  },
  range: {
    label: "users",
    status: "ok",
    strategy: "Mock index range scan on users.id",
    latencyMs: 4.4,
    rowsRead: 2,
    rowsReturned: 2,
    columns: ["id", "name", "tier"],
    rows: [
      { id: 2, name: "Jay", tier: "pro" },
      { id: 3, name: "Mia", tier: "pro" }
    ],
    notes: [
      "Browser fallback is active.",
      "Desktop mode will show the real range-scan plan from the Rust engine."
    ],
    source: "mock",
    planKind: "IndexRangeScan",
    message: "Mock response",
    astKind: "Select",
    statementCount: 1,
    rawAst: "{ kind: Select, table_name: users, filter: { column: id, op: >=, value: 2 }, limit: 2 }",
    rawPlan: "{ kind: IndexRangeScan, description: mock index range scan on users.id }",
    filterLabel: "id >= 2",
    limit: 2,
    usedIndexName: "idx_users_id"
  },
  orders: {
    label: "orders",
    status: "ok",
    strategy: "Mock lookup on orders.user_id",
    latencyMs: 5.2,
    rowsRead: 2,
    rowsReturned: 2,
    columns: ["id", "user_id", "amount_cents", "status"],
    rows: [
      { id: 101, user_id: 2, amount_cents: 4500, status: "pending" },
      { id: 102, user_id: 2, amount_cents: 1999, status: "paid" }
    ],
    notes: [
      "Order rows are mocked in browser mode.",
      "The Electron bridge will replace this with live engine output."
    ],
    source: "mock",
    planKind: "SeqScan",
    message: "Mock response",
    astKind: "Select",
    statementCount: 1,
    rawAst: "{ kind: Select, table_name: orders, limit: 2 }",
    rawPlan: "{ kind: SeqScan, description: mock lookup on orders.user_id }",
    filterLabel: "user_id = 2",
    limit: 2,
    usedIndexName: null
  },
  scan: {
    label: "audit_log",
    status: "warning",
    strategy: "Mock sequential scan on audit_log.action",
    latencyMs: 8.4,
    rowsRead: 3,
    rowsReturned: 2,
    columns: ["id", "entity", "action", "created_at"],
    rows: [
      { id: 9002, entity: "order", action: "delete", created_at: "2026-03-06T09:14:00Z" },
      { id: 9003, entity: "user", action: "delete", created_at: "2026-03-06T09:20:00Z" }
    ],
    notes: [
      "This path highlights scan-style feedback in the fallback runner.",
      "Live mode will surface real planner metadata."
    ],
    source: "mock",
    planKind: "SeqScan",
    message: "Mock response",
    astKind: "Select",
    statementCount: 1,
    rawAst: "{ kind: Select, table_name: audit_log, limit: 2 }",
    rawPlan: "{ kind: SeqScan, description: mock sequential scan on audit_log.action }",
    filterLabel: "action = delete",
    limit: 2,
    usedIndexName: null
  },
  explain: {
    label: "plan",
    status: "ok",
    strategy: "Mock EXPLAIN output",
    latencyMs: 1.8,
    rowsRead: 0,
    rowsReturned: 1,
    columns: ["plan"],
    rows: [{ plan: "index lookup via idx_users_id" }],
    notes: [
      "EXPLAIN works in desktop mode through the real Rust planner.",
      "This browser fallback is only for design iteration."
    ],
    source: "mock",
    planKind: "Explain",
    message: "Mock response",
    astKind: "Explain",
    statementCount: 1,
    rawAst: "{ kind: Explain }",
    rawPlan: "{ kind: Explain, used_index: idx_users_id }",
    filterLabel: "id = 2",
    limit: 1,
    usedIndexName: "idx_users_id"
  }
};

export const initialResult = sampleResults.default;
