const { spawnSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const rootDir = path.join(__dirname, "..");
const engineManifest = path.join(rootDir, "engine", "Cargo.toml");
const engineBinary = path.join(
  rootDir,
  "engine",
  "target",
  "debug",
  process.platform === "win32" ? "slatedb.exe" : "slatedb"
);
const dataDir = path.join(rootDir, "engine", "target", "workbench");
const dataPath = path.join(dataDir, "workbench.sdb");

const demoStatements = [
  "CREATE TABLE users (id INT, name TEXT, tier TEXT);",
  "INSERT INTO users VALUES (1, 'Ana', 'free');",
  "INSERT INTO users VALUES (2, 'Jay', 'pro');",
  "INSERT INTO users VALUES (3, 'Mia', 'pro');",
  "INSERT INTO users VALUES (4, 'Theo', 'free');",
  "CREATE TABLE orders (id INT, user_id INT, amount_cents INT, status TEXT);",
  "INSERT INTO orders VALUES (101, 2, 4500, 'pending');",
  "INSERT INTO orders VALUES (102, 2, 1999, 'paid');",
  "INSERT INTO orders VALUES (103, 3, 3200, 'pending');",
  "CREATE TABLE audit_log (id INT, entity TEXT, action TEXT, created_at TEXT);",
  "INSERT INTO audit_log VALUES (9001, 'order', 'create', '2026-03-06T09:11:00Z');",
  "INSERT INTO audit_log VALUES (9002, 'order', 'delete', '2026-03-06T09:14:00Z');",
  "INSERT INTO audit_log VALUES (9003, 'user', 'delete', '2026-03-06T09:20:00Z');",
  "CREATE INDEX idx_users_id ON users(id);",
  "CREATE INDEX idx_orders_user_id ON orders(user_id);",
  "CREATE INDEX idx_audit_action ON audit_log(action);"
];

function ensureEngineBinary() {
  if (fs.existsSync(engineBinary)) {
    return engineBinary;
  }

  const build = spawnSync("cargo", ["build", "--manifest-path", engineManifest], {
    cwd: rootDir,
    encoding: "utf8"
  });

  if (build.status !== 0) {
    throw new Error(build.stderr || build.stdout || "cargo build failed");
  }

  if (!fs.existsSync(engineBinary)) {
    throw new Error("slatedb binary was not produced by cargo build");
  }

  return engineBinary;
}

function runEngine(query) {
  ensureEngineBinary();
  fs.mkdirSync(dataDir, { recursive: true });
  const run = spawnSync(
    "cargo",
    [
      "run",
      "--quiet",
      "--manifest-path",
      engineManifest,
      "--",
      "exec",
      "--data",
      dataPath,
      "--format",
      "json",
      "--query",
      query
    ],
    {
      cwd: rootDir,
      encoding: "utf8"
    }
  );
  const stdout = (run.stdout || "").trim();
  if (stdout) {
    const parsed = JSON.parse(stdout);
    if (parsed.error) {
      const message = parsed.error.message || "engine returned an error";
      throw new Error(message);
    }
    return parsed;
  }
  if (run.status !== 0) {
    throw new Error(run.stderr || "cargo run failed");
  }
  throw new Error("engine produced no output");
}

function inspectSchema() {
  ensureEngineBinary();
  fs.mkdirSync(dataDir, { recursive: true });
  const run = spawnSync(
    "cargo",
    ["run", "--quiet", "--manifest-path", engineManifest, "--", "inspect", "--data", dataPath],
    {
      cwd: rootDir,
      encoding: "utf8"
    }
  );
  const stdout = (run.stdout || "").trim();
  if (!stdout) {
    throw new Error(run.stderr || "inspect produced no output");
  }
  const parsed = JSON.parse(stdout);
  if (parsed.error) {
    throw new Error(parsed.error.message || "inspect failed");
  }
  return parsed;
}

function ensureDemoDatabase() {
  if (fs.existsSync(dataPath)) {
    return;
  }
  for (const statement of demoStatements) {
    runEngine(statement);
  }
}

module.exports = {
  dataPath,
  ensureDemoDatabase,
  ensureEngineBinary,
  inspectSchema,
  runEngine
};
