const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("slatedbBridge", {
  runQuery: (query) => ipcRenderer.invoke("slatedb:run-query", query),
  getRuntimeInfo: () => ipcRenderer.invoke("slatedb:get-runtime-info"),
  getSchema: () => ipcRenderer.invoke("slatedb:get-schema")
});
