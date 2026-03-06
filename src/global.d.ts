import type {
  BridgeQueryBatchResponse,
  BridgeQueryResponse,
  BridgeRuntimeInfo,
  BridgeSchemaResponse
} from "./types";

declare global {
  interface Window {
    slatedbBridge?: {
      runQuery: (query: string) => Promise<BridgeQueryResponse | BridgeQueryBatchResponse>;
      getRuntimeInfo: () => Promise<BridgeRuntimeInfo>;
      getSchema: () => Promise<BridgeSchemaResponse>;
    };
  }
}

export {};
