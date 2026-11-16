import { expect, it } from "vitest";

import { createTestQueryClient, disposeTestQueryClients } from "@/test/query-client-lifecycle";

it("aborts owned pending reads and clears their cache before the next handler set", async () => {
  const client = createTestQueryClient();
  let aborted = false;
  let opened!: () => void;
  const started = new Promise<void>((resolve) => { opened = resolve; });
  const pending = client.fetchQuery({
    queryKey: ["owned-pending-read"],
    queryFn: ({ signal }) => new Promise<never>((_resolve, reject) => {
      signal.addEventListener("abort", () => { aborted = true; reject(new Error("aborted")); });
      opened();
    }),
  }).catch(() => undefined);
  await started;
  await disposeTestQueryClients();
  expect(aborted).toBe(true);
  expect(client.getQueryCache().getAll()).toEqual([]);
  await pending;
});
