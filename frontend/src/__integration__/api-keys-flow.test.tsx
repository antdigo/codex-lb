import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import App from "@/App";
import { ApiKeysSection } from "@/features/api-keys/components/api-keys-section";
import { renderWithProviders } from "@/test/utils";

function getParentRow(cell: HTMLElement): HTMLElement {
  const row = cell.closest("tr");
  if (!row) throw new Error("Expected element to be inside a table row");
  return row;
}

async function openRowActions(user: ReturnType<typeof userEvent.setup>, row: HTMLElement) {
  const actionsButton = within(row).getByRole("button", { name: "Actions" });
  await user.click(actionsButton);
}

async function openCreateDialog(user: ReturnType<typeof userEvent.setup>) {
  const button = await screen.findByRole("button", { name: "Create key" });
  await waitFor(() => expect(button).toBeEnabled());
  await user.click(button);
  return within(await screen.findByRole("dialog", { name: "Create API key" }));
}

function renderKeyManagement() {
  return renderWithProviders(<ApiKeysSection
    apiKeyAuthEnabled={false}
    hideUpstreamQuotaFromApiKeys={false}
    onApiKeyAuthEnabledChange={() => {}}
    onHideUpstreamQuotaFromApiKeysChange={() => {}}
  />);
}

describe("api keys flow integration", () => {
  it("creates a key and shows its plain value once", async () => {
    const user = userEvent.setup();
    const createdName = "Integration Key";

    renderKeyManagement();

    const createDialog = await openCreateDialog(user);
    await user.click(createDialog.getByLabelText("Name"));
    await user.paste(createdName);
    await user.click(createDialog.getByRole("button", { name: "Create" }));

    const createdDialog = await screen.findByRole("dialog", { name: "API key created" });
    expect(screen.getByText(/sk-test-generated/i)).toBeInTheDocument();
    const closeCandidates = within(createdDialog).getAllByRole("button", {
      name: "Close",
    });
    const closeButton =
      closeCandidates.find((element) => element.getAttribute("data-slot") === "button") ??
      closeCandidates[0];
    await user.click(closeButton);

    expect(await screen.findByText(createdName)).toBeInTheDocument();
  });

  it("edits an existing api key", async () => {
    const user = userEvent.setup();
    const updatedName = "Integration Key Updated";
    window.history.pushState({}, "", "/settings");
    renderWithProviders(<App />);
    const row = getParentRow(await screen.findByText("Default key"));
    await openRowActions(user, row);
    await user.click(await screen.findByRole("menuitem", { name: /Edit/ }));
    const dialog = within(await screen.findByRole("dialog", { name: "Edit API key" }));
    const input = dialog.getByLabelText("Name");
    await user.clear(input);
    await user.paste(updatedName);
    await user.click(dialog.getByRole("button", { name: "Save" }));
    expect(await screen.findByText(updatedName)).toBeInTheDocument();
    expect(screen.queryByText("Default key")).not.toBeInTheDocument();
  });

  it("deletes an existing api key after confirmation", async () => {
    const user = userEvent.setup();
    window.history.pushState({}, "", "/settings");
    renderWithProviders(<App />);
    const row = getParentRow(await screen.findByText("Read only key"));
    await openRowActions(user, row);
    await user.click(await screen.findByRole("menuitem", { name: /Delete/ }));
    const dialog = within(await screen.findByRole("alertdialog", { name: "Delete API key" }));
    await user.click(dialog.getByRole("button", { name: "Delete" }));
    await waitFor(() => expect(screen.queryByText("Read only key")).not.toBeInTheDocument());
    expect(screen.getByText("Default key")).toBeInTheDocument();
  });

  it("creates an api key with assigned accounts", async () => {
    const user = userEvent.setup();

    renderKeyManagement();

    const createDialog = await openCreateDialog(user);
    await user.click(createDialog.getByLabelText("Name"));
    await user.paste("Scoped Integration Key");
    await user.click(await screen.findByRole("button", { name: "All accounts" }));
    await user.click(screen.getByRole("menuitemcheckbox", { name: /primary@example\.com/i }));
    await user.keyboard("{Escape}");
    await user.click(createDialog.getByRole("button", { name: "Create" }));

    const createdDialog = await screen.findByRole("dialog", { name: "API key created" });
    const closeCandidates = within(createdDialog).getAllByRole("button", {
      name: "Close",
    });
    const closeButton =
      closeCandidates.find((element) => element.getAttribute("data-slot") === "button") ??
      closeCandidates[0];
    await user.click(closeButton);

    const createdRow = getParentRow(await screen.findByText("Scoped Integration Key"));
    await openRowActions(user, createdRow);
    await user.click(await screen.findByRole("menuitem", { name: /Edit/ }));

    expect(await screen.findByRole("button", { name: "1 account selected" })).toBeInTheDocument();
  });

  it("displays the current api key list on settings", async () => {
    window.history.pushState({}, "", "/settings");
    renderWithProviders(<App />);

    expect(await screen.findByRole("columnheader", { name: "Name" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Prefix" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Models" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Usage" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Expiry" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Status" })).toBeInTheDocument();

    const defaultKeyRow = getParentRow(screen.getByText("Default key"));
    expect(within(defaultKeyRow).getByText("sk-test")).toBeInTheDocument();
    expect(within(defaultKeyRow).getByText("gpt-5.1")).toBeInTheDocument();
    expect(within(defaultKeyRow).getByText("Tokens: 125K/1M weekly")).toBeInTheDocument();
    expect(within(defaultKeyRow).getByText("Active")).toBeInTheDocument();

    const readOnlyRow = getParentRow(screen.getByText("Read only key"));
    expect(within(readOnlyRow).getByText("sk-second")).toBeInTheDocument();
    expect(within(readOnlyRow).getByText("gpt-4o-mini")).toBeInTheDocument();
    expect(within(readOnlyRow).getByText(/12\.5K tok/)).toBeInTheDocument();
    expect(within(readOnlyRow).getByText(/2\.2K cached/)).toBeInTheDocument();
    expect(within(readOnlyRow).getByText(/42 req/)).toBeInTheDocument();
    expect(within(readOnlyRow).getByText(/\$0\.42/)).toBeInTheDocument();
    expect(within(readOnlyRow).getByText("Never")).toBeInTheDocument();
    expect(within(readOnlyRow).getByText("Disabled")).toBeInTheDocument();
  });

  it("shows usage bars when editing a key with limits", async () => {
    const user = userEvent.setup({ delay: null });

    window.history.pushState({}, "", "/settings");
    renderWithProviders(<App />);

    expect(await screen.findByText("Default key")).toBeInTheDocument();
    const defaultKeyRow = getParentRow(screen.getByText("Default key"));
    await openRowActions(user, defaultKeyRow);
    await user.click(await screen.findByRole("menuitem", { name: /Edit/ }));

    // Edit dialog should show current usage section
    expect(await screen.findByText("Current usage")).toBeInTheDocument();
  });
});
