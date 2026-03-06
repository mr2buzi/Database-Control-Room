import { useEffect, useMemo, useState } from "react";
import { defaultQuery, fallbackSchema, initialHistory, initialResult, insights } from "./data";
import { getRuntimeInfo, getSchema, runQuery } from "./queryEngine";
import type { QueryResult, QueryRun, Table } from "./types";

const presets = [
  {
    label: "User lookup",
    description: "Indexed equality query on users.id",
    query: defaultQuery
  },
  {
    label: "Order activity",
    description: "Operational lookup on orders.user_id",
    query: `SELECT id, user_id, amount_cents, status
FROM orders
WHERE user_id = 2
LIMIT 2;`
  },
  {
    label: "Audit scan",
    description: "Equality filter over audit_log.action",
    query: `SELECT id, entity, action, created_at
FROM audit_log
WHERE action = 'delete'
LIMIT 2;`
  },
  {
    label: "Explain plan",
    description: "Inspect the real planner output",
    query: `EXPLAIN SELECT id, name, tier
FROM users
WHERE id = 2
LIMIT 1;`
  },
  {
    label: "Txn demo",
    description: "Run a multi-statement transaction",
    query: `BEGIN;
INSERT INTO users VALUES (5, 'Rina', 'pro');
ROLLBACK;
SELECT id, name, tier
FROM users
WHERE id = 5;`
  }
];

const metricToneClass = {
  good: "tone-good",
  neutral: "tone-neutral",
  warn: "tone-warn"
};

export function App() {
  const [query, setQuery] = useState(defaultQuery);
  const [history, setHistory] = useState<QueryRun[]>(initialHistory);
  const [selectedHistoryId, setSelectedHistoryId] = useState<number>(initialHistory[0].id);
  const [result, setResult] = useState<QueryResult>(initialResult);
  const [resultSnapshots, setResultSnapshots] = useState<Record<number, QueryResult>>({
    [initialHistory[0].id]: initialResult
  });
  const [isRunning, setIsRunning] = useState(false);
  const [isRefreshingSchema, setIsRefreshingSchema] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [runtimeMode, setRuntimeMode] = useState("Browser fallback");
  const [runtimeFocus, setRuntimeFocus] = useState("Mock adapter for design iteration");
  const [schema, setSchema] = useState<Table[]>(fallbackSchema);

  useEffect(() => {
    let active = true;
    getRuntimeInfo()
      .then((info) => {
        if (!active || !info) {
          return;
        }
        setRuntimeMode("Desktop engine");
        setRuntimeFocus(`Electron IPC -> ${info.dataPath}`);
        getSchema()
          .then((tables) => {
            if (active && tables && tables.length > 0) {
              setSchema(tables);
            }
          })
          .catch(() => undefined);
      })
      .catch(() => {
        if (!active) {
          return;
        }
        setRuntimeMode("Browser fallback");
        setRuntimeFocus("Mock adapter for design iteration");
      });
    return () => {
      active = false;
    };
  }, []);

  const selectedRun = useMemo(
    () => history.find((item) => item.id === selectedHistoryId) ?? history[0],
    [history, selectedHistoryId]
  );

  useEffect(() => {
    const snapshot = resultSnapshots[selectedHistoryId];
    if (snapshot) {
      setResult(snapshot);
    }
  }, [resultSnapshots, selectedHistoryId]);

  const refreshSchema = async () => {
    setIsRefreshingSchema(true);
    try {
      const tables = await getSchema();
      if (tables && tables.length > 0) {
        setSchema(tables);
      }
    } finally {
      setIsRefreshingSchema(false);
    }
  };

  const execute = async () => {
    const nextId = history[0] ? history[0].id + 1 : 1;
    setIsRunning(true);
    setErrorMessage(null);
    try {
      const next = await runQuery(query, nextId);
      setResult(next.result);
      setResultSnapshots((current) => ({
        ...current,
        [next.history.id]: next.result
      }));
      setHistory((current) => [next.history, ...current]);
      setSelectedHistoryId(next.history.id);
      if (next.result.source === "engine") {
        await refreshSchema();
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Query execution failed.");
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="shell">
      <div className="ambient ambient-left" />
      <div className="ambient ambient-right" />
      <header className="topbar panel">
        <div>
          <p className="eyebrow">Human-Centered Database Workbench</p>
          <h1>SlateDB Control Room</h1>
        </div>
        <div className="topbar-meta">
          <div>
            <span className="meta-label">Mode</span>
            <strong>{runtimeMode}</strong>
          </div>
          <div>
            <span className="meta-label">Source</span>
            <strong>{result.source === "engine" ? "Rust CLI" : "Browser mock"}</strong>
          </div>
          <div>
            <span className="meta-label">Focus</span>
            <strong>{runtimeFocus}</strong>
          </div>
        </div>
      </header>

      <main className="layout">
        <aside className="sidebar panel">
          <section>
            <div className="panel-header compact-header">
              <div>
                <p className="section-label">Schema atlas</p>
                <h2>Live catalog</h2>
              </div>
              <button className="ghost-button" onClick={refreshSchema} disabled={isRefreshingSchema}>
                {isRefreshingSchema ? "Refreshing..." : "Refresh"}
              </button>
            </div>
            <div className="schema-list">
              {schema.map((table) => (
                <article key={table.name} className="schema-card">
                  <div className="schema-header">
                    <h2>{table.name}</h2>
                    <span>{table.rowCount.toLocaleString()} rows</span>
                  </div>
                  <ul>
                    {table.columns.map((column) => (
                      <li key={column.name}>
                        <span>{column.name}</span>
                        <small>
                          {column.type}
                          {column.indexed ? " | idx" : ""}
                        </small>
                      </li>
                    ))}
                  </ul>
                </article>
              ))}
            </div>
          </section>

          <section>
            <p className="section-label">Planner signals</p>
            <div className="insight-grid">
              {insights.map((card) => (
                <article key={card.title} className={`insight-card ${metricToneClass[card.tone]}`}>
                  <span>{card.title}</span>
                  <strong>{card.value}</strong>
                  <p>{card.detail}</p>
                </article>
              ))}
            </div>
          </section>
        </aside>

        <section className="workspace">
          <div className="workspace-top">
            <section className="panel editor-panel">
              <div className="panel-header">
                <div>
                  <p className="section-label">Query studio</p>
                  <h2>Compose and inspect</h2>
                </div>
                <button className="primary-button" onClick={execute} disabled={isRunning}>
                  {isRunning ? "Running..." : "Run query"}
                </button>
              </div>

              <div className="preset-row">
                {presets.map((preset) => (
                  <button
                    key={preset.label}
                    className="preset-chip"
                    onClick={() => setQuery(preset.query)}
                  >
                    <strong>{preset.label}</strong>
                    <span>{preset.description}</span>
                  </button>
                ))}
              </div>

              <label className="editor-shell">
                <span className="editor-label">Active statement</span>
                <textarea
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  spellCheck={false}
                />
              </label>

              {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}
              {result.message ? <div className="info-banner">{result.message}</div> : null}

              <div className="status-strip">
                <div>
                  <span className="meta-label">Execution strategy</span>
                  <strong>{result.strategy}</strong>
                </div>
                <div>
                  <span className="meta-label">Latency</span>
                  <strong>{result.latencyMs.toFixed(1)} ms</strong>
                </div>
                <div>
                  <span className="meta-label">Rows read</span>
                  <strong>{result.rowsRead.toLocaleString()}</strong>
                </div>
                <div>
                  <span className="meta-label">Rows returned</span>
                  <strong>{result.rowsReturned.toLocaleString()}</strong>
                </div>
                <div>
                  <span className="meta-label">Statements</span>
                  <strong>{result.statementCount}</strong>
                </div>
              </div>
            </section>

            <section className="panel explain-panel">
              <div className="panel-header">
                <div>
                  <p className="section-label">Run narrative</p>
                  <h2>Why the engine chose this path</h2>
                </div>
              </div>

              <article className={`hero-metric ${result.status === "warning" ? "warning" : ""}`}>
                <span>{result.status === "warning" ? "Needs attention" : "Healthy run"}</span>
                <strong>{result.strategy}</strong>
                <p>
                  {result.status === "warning"
                    ? "The current statement read more rows than it returned. That is useful HCI feedback when you want to discuss indexing, planner choices, or query refinement."
                    : "The workbench is showing the planner and execution story coming from the active runtime, which makes the interface useful for both demos and interview walkthroughs."}
                </p>
              </article>

              <div className="notes-list">
                {result.notes.map((note) => (
                  <div key={note} className="note-item">
                    <span className="note-marker" />
                    <p>{note}</p>
                  </div>
                ))}
              </div>

              <div className="mini-steps">
                <div>
                  <span>Parse</span>
                  <strong>{result.astKind}</strong>
                </div>
                <div>
                  <span>Plan</span>
                  <strong>{result.status === "warning" ? "Scan-heavy path" : "Planner aligned"}</strong>
                </div>
                <div>
                  <span>Execute</span>
                  <strong>{result.rowsReturned} row(s) materialized</strong>
                </div>
              </div>

              <div className="trace-grid">
                <div className="trace-card">
                  <span className="section-label">Execution detail</span>
                  <div className="trace-tags">
                    <span className="trace-tag">{result.planKind}</span>
                    <span className="trace-tag">{result.source}</span>
                    {result.usedIndexName ? <span className="trace-tag">idx: {result.usedIndexName}</span> : null}
                    {result.filterLabel ? <span className="trace-tag">{result.filterLabel}</span> : null}
                    {typeof result.limit === "number" ? <span className="trace-tag">limit {result.limit}</span> : null}
                  </div>
                </div>
              </div>
            </section>
          </div>

          <div className="workspace-bottom">
            <section className="panel results-panel">
              <div className="panel-header">
                <div>
                  <p className="section-label">Result surface</p>
                  <h2>{result.label}</h2>
                </div>
              </div>

              <div className="table-shell">
                <table>
                  <thead>
                    <tr>
                      {result.columns.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, index) => (
                      <tr key={`${result.label}-${index}`}>
                        {result.columns.map((column) => (
                          <td key={column}>{String(row[column] ?? "")}</td>
                        ))}
                      </tr>
                    ))}
                    {result.rows.length === 0 ? (
                      <tr>
                        <td className="empty-state" colSpan={Math.max(result.columns.length, 1)}>
                          No rows returned for this statement.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="panel trace-panel">
              <div className="panel-header">
                <div>
                  <p className="section-label">Engine trace</p>
                  <h2>AST and planner payload</h2>
                </div>
              </div>

              <div className="trace-shell">
                <div className="trace-block">
                  <span className="section-label">AST</span>
                  <pre>{result.rawAst}</pre>
                </div>
                <div className="trace-block">
                  <span className="section-label">Plan</span>
                  <pre>{result.rawPlan}</pre>
                </div>
              </div>
            </section>
          </div>

          <section className="panel history-panel">
            <div className="panel-header">
              <div>
                <p className="section-label">Session timeline</p>
                <h2>Recent runs</h2>
              </div>
            </div>

            <div className="history-list">
              {history.map((entry) => (
                <button
                  key={entry.id}
                  className={`history-item ${entry.id === selectedRun.id ? "active" : ""}`}
                  onClick={() => setSelectedHistoryId(entry.id)}
                >
                  <div className="history-topline">
                    <strong>{entry.summary}</strong>
                    <span>{entry.timestamp}</span>
                  </div>
                  <p>{entry.strategy}</p>
                  <div className="history-metrics">
                    <span>{entry.latencyMs.toFixed(1)} ms</span>
                    <span>{entry.rowsRead.toLocaleString()} read</span>
                    <span>{entry.rowsReturned.toLocaleString()} out</span>
                    <span>{entry.statementCount} stmt</span>
                    <span>{entry.source}</span>
                  </div>
                </button>
              ))}
            </div>
          </section>
        </section>
      </main>
    </div>
  );
}
