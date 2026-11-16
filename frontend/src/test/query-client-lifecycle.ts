import { QueryClient } from "@tanstack/react-query";

const ownedClients = new Set<QueryClient>();

export function createTestQueryClient(): QueryClient {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
  ownedClients.add(client);
  return client;
}

export async function disposeTestQueryClients(): Promise<void> {
  const clients = [...ownedClients];
  ownedClients.clear();
  await Promise.all(clients.map(async (client) => {
    await client.cancelQueries();
    client.clear();
  }));
}
