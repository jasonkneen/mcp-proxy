import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import http from "http";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  CallToolRequestSchema,
  CompleteRequestSchema,
  GetPromptRequestSchema,
  JSONRPCMessage,
  ListPromptsRequestSchema,
  ListResourcesRequestSchema,
  ListResourceTemplatesRequestSchema,
  ListToolsRequestSchema,
  LoggingMessageNotificationSchema,
  ReadResourceRequestSchema,
  ServerCapabilities,
} from "@modelcontextprotocol/sdk/types.js";
import { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";

type TransportEvent =
  | {
      type: "close";
    }
  | {
      type: "onclose";
    }
  | {
      type: "onerror";
      error: Error;
    }
  | {
      type: "onmessage";
      message: JSONRPCMessage;
    }
  | {
      type: "send";
      message: JSONRPCMessage;
    }
  | {
      type: "start";
    };

export const tapTransport = (
  transport: Transport,
  eventHandler: (event: TransportEvent) => void,
) => {
  const originalClose = transport.close.bind(transport);
  const originalOnClose = transport.onclose?.bind(transport);
  const originalOnError = transport.onerror?.bind(transport);
  const originalOnMessage = transport.onmessage?.bind(transport);
  const originalSend = transport.send.bind(transport);
  const originalStart = transport.start.bind(transport);

  transport.close = async () => {
    eventHandler({
      type: "close",
    });

    return originalClose?.();
  };

  transport.onclose = async () => {
    eventHandler({
      type: "onclose",
    });

    return originalOnClose?.();
  };

  transport.onerror = async (error: Error) => {
    eventHandler({
      type: "onerror",
      error,
    });

    return originalOnError?.(error);
  };

  transport.onmessage = async (message: JSONRPCMessage) => {
    eventHandler({
      type: "onmessage",
      message,
    });

    return originalOnMessage?.(message);
  };

  transport.send = async (message: JSONRPCMessage) => {
    eventHandler({
      type: "send",
      message,
    });

    return originalSend?.(message);
  };

  transport.start = async () => {
    eventHandler({
      type: "start",
    });

    return originalStart?.();
  };

  return transport;
};

export const proxyServer = async ({
  server,
  client,
  serverCapabilities,
}: {
  server: Server;
  client: Client;
  serverCapabilities: ServerCapabilities;
}) => {
  if (serverCapabilities?.logging) {
    server.setNotificationHandler(
      LoggingMessageNotificationSchema,
      async (args) => {
        return client.notification(args);
      },
    );
  }

  if (serverCapabilities?.prompts) {
    server.setRequestHandler(GetPromptRequestSchema, async (args) => {
      return client.getPrompt(args.params);
    });

    server.setRequestHandler(ListPromptsRequestSchema, async (args) => {
      return client.listPrompts(args.params);
    });
  }

  if (serverCapabilities?.resources) {
    server.setRequestHandler(ListResourcesRequestSchema, async (args) => {
      return client.listResources(args.params);
    });

    server.setRequestHandler(
      ListResourceTemplatesRequestSchema,
      async (args) => {
        return client.listResourceTemplates(args.params);
      },
    );

    server.setRequestHandler(ReadResourceRequestSchema, async (args) => {
      return client.readResource(args.params);
    });
  }

  if (serverCapabilities?.tools) {
    server.setRequestHandler(CallToolRequestSchema, async (args) => {
      return client.callTool(args.params);
    });

    server.setRequestHandler(ListToolsRequestSchema, async (args) => {
      return client.listTools(args.params);
    });
  }

  server.setRequestHandler(CompleteRequestSchema, async (args) => {
    return client.complete(args.params);
  });
};

export type SSEServer = {
  close: () => Promise<void>;
};

type ServerLike = {
  connect: Server["connect"];
  close: Server["close"];
};

export const startSSEServer = async <T extends ServerLike>({
  port,
  createServer,
  endpoint,
  onConnect,
  onClose,
}: {
  port: number;
  endpoint: string;
  createServer: (request: http.IncomingMessage) => Promise<T>;
  onConnect?: (server: T) => void;
  onClose?: (server: T) => void;
}): Promise<SSEServer> => {
  const activeTransports: Record<string, SSEServerTransport> = {};

  /**
   * @author https://dev.classmethod.jp/articles/mcp-sse/
   */
  const httpServer = http.createServer(async (req, res) => {
    if (req.headers.origin) {
      try {
        const origin = new URL(req.headers.origin);

        res.setHeader("Access-Control-Allow-Origin", origin.origin);
        res.setHeader("Access-Control-Allow-Credentials", "true");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.setHeader("Access-Control-Allow-Headers", "*");
      } catch (error) {
        console.error("Error parsing origin:", error);
      }
    }

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === "GET" && req.url === `/ping`) {
      res.writeHead(200).end("pong");

      return;
    }

    if (req.method === "GET" && req.url === endpoint) {
      const transport = new SSEServerTransport("/messages", res);

      let server: T;

      try {
        server = await createServer(req);
      } catch (error) {
        if (error instanceof Response) {
          res.writeHead(error.status).end(error.statusText);

          return;
        }

        res.writeHead(500).end("Error creating server");

        return;
      }

      activeTransports[transport.sessionId] = transport;

      let closed = false;

      res.on("close", async () => {
        closed = true;

        try {
          await server.close();
        } catch (error) {
          console.error("Error closing server:", error);
        }

        delete activeTransports[transport.sessionId];

        onClose?.(server);
      });

      try {
        await server.connect(transport);

        await transport.send({
          jsonrpc: "2.0",
          method: "sse/connection",
          params: { message: "SSE Connection established" },
        });

        onConnect?.(server);
      } catch (error) {
        if (!closed) {
          console.error("Error connecting to server:", error);
          
          res.writeHead(500).end("Error connecting to server");
        }
      }

      return;
    }

    if (req.method === "POST" && req.url?.startsWith("/messages")) {
      const sessionId = new URL(
        req.url,
        "https://example.com",
      ).searchParams.get("sessionId");

      if (!sessionId) {
        res.writeHead(400).end("No sessionId");

        return;
      }

      const activeTransport: SSEServerTransport | undefined =
        activeTransports[sessionId];

      if (!activeTransport) {
        res.writeHead(400).end("No active transport");

        return;
      }

      await activeTransport.handlePostMessage(req, res);

      return;
    }

    res.writeHead(404).end();
  });

  await new Promise((resolve) => {
    httpServer.listen(port, "::", () => {
      resolve(undefined);
    });
  });

  return {
    close: async () => {
      for (const transport of Object.values(activeTransports)) {
        await transport.close();
      }

      return new Promise((resolve, reject) => {
        httpServer.close((error) => {
          if (error) {
            reject(error);

            return;
          }

          resolve();
        });
      });
    },
  };
};
