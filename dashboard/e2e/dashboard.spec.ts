import { test, expect, Page } from "@playwright/test";

// Fulfil a proxied Hookline GET with JSON.
async function mockGet(page: Page, suffix: string, body: unknown) {
  await page.route(`**/api/hookline/${suffix}`, async (route) => {
    if (route.request().method() !== "GET") return route.fallback();
    await route.fulfill({ json: body as object });
  });
}

test("deliveries page renders rows and filters by outcome", async ({ page }) => {
  await mockGet(page, "v1/deliveries", {
    deliveries: [
      {
        id: "a1", message_id: "m1", event_id: "evt-delivered-001",
        endpoint: "https://consumer.test/hook", attempt: 1,
        outcome: "delivered", status_code: 200, duration_ms: 11,
        at: "2026-06-14T12:00:00Z",
      },
    ],
  });
  // The "retrying" filter requests a filtered list.
  await page.route("**/api/hookline/v1/deliveries?outcome=retrying", (route) =>
    route.fulfill({
      json: {
        deliveries: [
          {
            id: "b2", message_id: "m2", event_id: "evt-retry-002",
            endpoint: "https://flaky.test/hook", attempt: 3,
            outcome: "retrying", status_code: 503, duration_ms: 42,
            at: "2026-06-14T12:01:00Z",
          },
        ],
      },
    })
  );

  await page.goto("/");
  const table = page.getByTestId("deliveries-table");
  await expect(table).toBeVisible();
  await expect(table.getByText("consumer.test")).toBeVisible();
  await expect(table.locator(".badge.delivered")).toBeVisible();

  await page.getByTestId("filter-retrying").click();
  await expect(table.getByText("flaky.test")).toBeVisible();
});

test("dead-letter queue lists entries and replays one", async ({ page }) => {
  let replayed = false;
  await page.route("**/api/hookline/v1/dlq", (route) =>
    route.fulfill({
      json: {
        dead_letters: replayed
          ? []
          : [
              {
                message_id: "dead-1", event_id: "evt-dead-001",
                endpoint: "https://down.test/hook", attempts: 12,
                reason: "exhausted after 12 attempts: last status 500",
                payload: { order: 7 }, failed_at: "2026-06-14T11:00:00Z",
              },
            ],
      },
    })
  );
  await page.route("**/api/hookline/v1/dlq/dead-1/replay", (route) => {
    replayed = true;
    return route.fulfill({ json: { id: "new-msg", event_id: "evt-dead-001" } });
  });

  await page.goto("/dlq");
  await expect(page.getByTestId("dlq-table")).toBeVisible();
  await expect(page.getByText("evt-dead-001")).toBeVisible();

  await page.getByTestId("replay-dead-1").click();
  await expect(page.getByTestId("replay-notice")).toContainText("evt-dead-001");
  // After replay the list reloads empty.
  await expect(page.getByTestId("dlq-empty")).toBeVisible();
});

test("endpoints page registers an endpoint and shows the secret once", async ({ page }) => {
  let registered = false;
  await page.route("**/api/hookline/v1/endpoints", (route) => {
    if (route.request().method() === "POST") {
      registered = true;
      return route.fulfill({
        json: {
          id: "ep-1", url: "https://new.test/hook", producer: "default",
          disabled: false, created_at: "2026-06-14T10:00:00Z",
          secret: "whsec_supersecretvalue",
        },
      });
    }
    return route.fulfill({
      json: {
        endpoints: registered
          ? [{ id: "ep-1", url: "https://new.test/hook", producer: "default", disabled: false, created_at: "2026-06-14T10:00:00Z" }]
          : [],
      },
    });
  });

  await page.goto("/endpoints");
  await page.getByTestId("endpoint-url").fill("https://new.test/hook");
  await page.getByTestId("register").click();

  await expect(page.getByTestId("secret-notice")).toContainText("whsec_supersecretvalue");
  await expect(page.getByTestId("endpoints-table")).toContainText("https://new.test/hook");
});
