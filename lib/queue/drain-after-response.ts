import { after } from "next/server";
import { drainQueue, isInlineWorkerEnabled } from "@/lib/queue/drain";

// Routes that call this must export a maxDuration of at least this budget plus
// the drain's 60s close grace (160s in the webhook routes).
const WEBHOOK_DRAIN_BUDGET_MS = 90_000;

/**
 * After the response is sent, send what the request just enqueued. Webhook
 * senders get their 200 straight away; the DM goes out in seconds instead of
 * waiting for the next /api/cron/worker tick. No-op unless the inline worker is
 * enabled (by default only on Vercel).
 */
export function drainAfterResponse(): void {
  if (!isInlineWorkerEnabled()) return;
  after(async () => {
    try {
      await drainQueue({ budgetMs: WEBHOOK_DRAIN_BUDGET_MS });
    } catch (error) {
      console.error(
        "[DM Worker] Inline drain failed:",
        error instanceof Error ? error.message : error
      );
    }
  });
}
