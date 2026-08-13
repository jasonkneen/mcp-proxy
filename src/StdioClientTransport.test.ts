import { JSONRPCMessage } from "@modelcontextprotocol/client";
import { ChildProcess } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { expect, it } from "vitest";

import { StdioClientTransport } from "./StdioClientTransport.js";

/**
 * Reaches for the child process the transport keeps private, so the test can
 * watch the stdin stream the send path writes to.
 */
const childOf = (transport: StdioClientTransport): ChildProcess =>
  (transport as unknown as { _process: ChildProcess })._process;

const messageOfSize = (id: number, bytes: number): JSONRPCMessage => ({
  id,
  jsonrpc: "2.0",
  method: "tools/call",
  params: { arguments: { blob: "x".repeat(bytes) }, name: "noop" },
});

it("settles pending sends when the child exits before the write drains", async () => {
  // Regression test: send() resolved on "drain" and nothing else. A child
  // that stops reading stdin makes write() return false, and if that child
  // then dies the "drain" event never arrives - so the promise stayed pending
  // forever and its "drain" listener stayed attached, one per stuck send.
  const transport = new StdioClientTransport({
    // Never reads stdin, so the pipe backs up and write() returns false.
    args: ["-e", "setTimeout(() => {}, 60000)"],
    command: "node",
    env: process.env as Record<string, string>,
    stderr: "ignore",
  });

  await transport.start();

  const child = childOf(transport);
  const stdin = child.stdin!;
  // spawn() attaches its own "exit" listener for the abort signal, so the
  // interesting number is the delta, not the absolute count.
  const exitListenersBefore = child.listenerCount("exit");

  const outcomes = [1, 2, 3].map((id) =>
    transport.send(messageOfSize(id, 1024 * 1024)).then(
      () => "resolved" as const,
      () => "rejected" as const,
    ),
  );

  // Precondition: every write was buffered rather than flushed, which is the
  // only situation the bug applies to. The promise executor runs
  // synchronously, so the listeners are already attached here.
  expect(stdin.writableLength).toBeGreaterThan(0);
  expect(stdin.listenerCount("drain")).toBe(3);

  process.kill(transport.pid!, "SIGKILL");

  const settled = await Promise.race([
    Promise.all(outcomes),
    delay(2_000).then(() => "still pending" as const),
  ]);

  expect(settled).toEqual(["rejected", "rejected", "rejected"]);
  expect(stdin.listenerCount("drain")).toBe(0);
  expect(child.listenerCount("exit")).toBe(exitListenersBefore);

  await transport.close();
}, 10_000);

it("settles a pending send when the transport is closed", async () => {
  // The same hang on the ordinary shutdown path: close() aborts the child, so
  // a write that has not drained yet can never complete.
  const transport = new StdioClientTransport({
    args: ["-e", "setTimeout(() => {}, 60000)"],
    command: "node",
    env: process.env as Record<string, string>,
    stderr: "ignore",
  });

  await transport.start();

  const stdin = childOf(transport).stdin!;

  const outcome = transport.send(messageOfSize(1, 1024 * 1024)).then(
    () => "resolved" as const,
    () => "rejected" as const,
  );

  expect(stdin.listenerCount("drain")).toBe(1);

  await transport.close();

  await expect(
    Promise.race([outcome, delay(2_000).then(() => "still pending" as const)]),
  ).resolves.toBe("rejected");
  expect(stdin.listenerCount("drain")).toBe(0);
}, 10_000);

it("resolves send() and leaves no listeners behind once a buffered write drains", async () => {
  // The happy path for the same code: a child that does read stdin lets the
  // buffered write flush, so "drain" arrives and the promise resolves - and
  // the listeners added for the failure paths must be removed too.
  const transport = new StdioClientTransport({
    args: ["-e", "process.stdin.resume(); setTimeout(() => {}, 60000)"],
    command: "node",
    env: process.env as Record<string, string>,
    stderr: "ignore",
  });

  await transport.start();

  const child = childOf(transport);
  const stdin = child.stdin!;
  const exitListenersBefore = child.listenerCount("exit");

  const send = transport.send(messageOfSize(1, 1024 * 1024));

  expect(stdin.listenerCount("drain")).toBe(1);

  await expect(send).resolves.toBeUndefined();

  expect(stdin.listenerCount("drain")).toBe(0);
  expect(child.listenerCount("exit")).toBe(exitListenersBefore);

  await transport.close();
}, 10_000);
