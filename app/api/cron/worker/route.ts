import { NextRequest, NextResponse } from "next/server";
import { drainQueue } from "@/lib/queue/drain";

/**
 * The DM worker for serverless deploys. Runs every minute (see vercel.json):
 * sends whatever is due in the queue — delayed retries, follow-ups, paced
 * comments, anything a webhook-triggered run did not finish — and runs the
 * missed-comment sweep every COMMENT_POLL_INTERVAL_MS. Webhooks drain the
 * queue themselves for immediate sends; this is what catches everything else.
 * Not needed when `npm run worker` runs on an always-on host.
 */

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

// Leaves maxDuration headroom for closing the worker on jobs still in flight.
const BUDGET_MS = 230_000;

export async function GET(request: NextRequest) {
  const authHeader = request.headers.get("authorization");
  const cronSecret = process.env.CRON_SECRET || process.env.NEXTAUTH_SECRET;

  if (authHeader !== `Bearer ${cronSecret}`) {
    return NextResponse.json(
      { success: false, error: "Unauthorized" },
      { status: 401 }
    );
  }

  const result = await drainQueue({ budgetMs: BUDGET_MS, poll: true });
  return NextResponse.json({ success: true, data: result });
}
