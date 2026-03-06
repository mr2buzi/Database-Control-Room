const { app, BrowserWindow, ipcMain } = require("electron");
const path = require("path");
const runtime = require("./slatedb-runtime.cjs");

const rootDir = path.join(__dirname, "..");

function createWindow() {
  const window = new BrowserWindow({
    width: 1560,
    height: 980,
    minWidth: 1100,
    minHeight: 760,
    backgroundColor: "#131116",
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  const devUrl = process.env.SLATEDB_DEV_SERVER_URL;
  if (devUrl) {
    window.loadURL(devUrl);
  } else {
    window.loadFile(path.join(rootDir, "dist", "index.html"));
  }
}

ipcMain.handle("slatedb:run-query", async (_event, query) => runtime.runEngine(query));
ipcMain.handle("slatedb:get-runtime-info", async () => ({
  mode: "desktop-engine",
  dataPath: runtime.dataPath
}));
ipcMain.handle("slatedb:get-schema", async () => runtime.inspectSchema());

app.whenReady().then(() => {
  runtime.ensureDemoDatabase();
  createWindow();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});
