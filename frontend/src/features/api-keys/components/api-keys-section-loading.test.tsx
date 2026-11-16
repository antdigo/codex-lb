import { Suspense } from "react";
import { act, screen } from "@testing-library/react";
import { expect, it, vi } from "vitest";

import { renderWithProviders } from "@/test/utils";
import { ApiKeysSection } from "@/features/api-keys/components/api-keys-section";

const deferred = vi.hoisted(() => {
  let resolve!: (value: { ApiKeyCreateDialog: () => null }) => void;
  const promise = new Promise<{ ApiKeyCreateDialog: () => null }>((done) => { resolve = done; });
  return { promise, resolve };
});
vi.mock("@/features/api-keys/components/api-key-create-dialog", () => deferred.promise);

it("keeps key controls visible while a closed lazy dialog is loading", async () => {
  try {
    renderWithProviders(
      <Suspense fallback={<p>Whole page suspended</p>}>
        <ApiKeysSection apiKeyAuthEnabled={false} hideUpstreamQuotaFromApiKeys={false}
          onApiKeyAuthEnabledChange={() => {}} onHideUpstreamQuotaFromApiKeysChange={() => {}} />
      </Suspense>,
    );
    expect(screen.getByRole("button", { name: "Create key" })).toBeInTheDocument();
    expect(screen.queryByText("Whole page suspended")).not.toBeInTheDocument();
  } finally {
    await act(async () => { deferred.resolve({ ApiKeyCreateDialog: () => null }); });
  }
});
