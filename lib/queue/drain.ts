/**
 * Serverless queue drain.
 *
 * `npm run worker` keeps a BullMQ worker up forever. A Vercel function cannot,
 * so here the same worker (`createDMWorker`, same processor, retries and
 * failure alerts) is started for one invocation, run while there is work that
 * is due, and closed before the function's time runs out. Two things start a
 * run: the webhook routes, right after a comment or message is enqueued, and
 * the per-minute `/api/cron/worker` route, which picks up delayed jobs
 * (retries, follow-ups, paced comments) and runs the missed-comment sweep.
 *
 * A function killed mid-job leaves that job "active"; the next run's stalled
 * check moves it back to waiting, and the durable delivery claim keeps an
 * uncertain send from being repeated.
 */

import os from "node:os";
import { randomUUID } from "node:crypto";
import { getDMQueue, getRedisConnection } from "@/lib/queue/client";
import { createDMWorker } from "@/lib/queue/dm-worker";
import { recordWorkerHeartbeat } from "@/lib/ops/worker-health";
import { reconcileComments } from "@/lib/polling/comment-reconciler";
import { attachPendingNextReels } from "@/lib/automation/attach-next-reel";

// One drain at a time. Every webhook asks for a run; without the lock a burst
// of comments would start a worker (and its Redis connections) per request.
const DRAIN_LOCK_KEY = "lock:worker:dm-drain";
const POLL_GATE_KEY = "gate:worker:comment-poll";
// Time left after the budget for `worker.close()` to finish jobs in flight.
const CLOSE_GRACE_MS = 60_000;
const HEARTBEAT_INTERVAL_MS = 30_000;
const DEFAULT_CHECK_INTERVAL_MS = 2_000;
// A delayed job due further out than this is left for the next cron tick
// (every minute) instead of keeping this function alive to wait for it.
const DEFAULT_LINGER_MS = 60_000;
// After releasing the lock, a job enqueued while this run was stopping could
// have been skipped by a webhook that found the lock held. Look again a few
// times so it is not left until the next tick.
const MAX_RELOCK_PASSES = 3;

const RELEASE_LOCK_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`;

export interface DrainOptions {
  // How long this run may keep taking jobs, counted from the call.
  budgetMs: number;
  // Keep running while a delayed job falls due within this window.
  lingerMs?: number;
  // Also run the missed-comment sweep and next-reel attachment, at most once
  // per COMMENT_POLL_INTERVAL_MS across all runs.
  poll?: boolean;
  checkIntervalMs?: number;
}

export interface DrainResult {
  status: "drained" | "idle" | "busy";
  passes: number;
  polled: boolean;
  ranMs: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function formatError(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error";
}

// Whether the webhook routes should drain the queue themselves. On by default
// on Vercel, where nothing else consumes it; off elsewhere, where
// `npm run worker` does. INLINE_WORKER=true|false overrides either way.
export function isInlineWorkerEnabled(): boolean {
  const flag = process.env.INLINE_WORKER;
  if (flag) return flag === "true";
  return process.env.VERCEL === "1";
}

/**
 * When the next job can run: now if anything is waiting or active, the
 * earliest delayed job's due time otherwise, null when the queue is empty.
 */
export async function nextWorkAt(now = Date.now()): Promise<number | null> {
  const queue = getDMQueue();
  const counts = await queue.getJobCounts("waiting", "active", "prioritized");
  const ready =
    (counts.waiting ?? 0) + (counts.active ?? 0) + (counts.prioritized ?? 0);
  if (ready > 0) return now;

  // BullMQ scores delayed jobs as dueTimestamp * 0x1000 + a counter.
  const [, score] = await getRedisConnection().zrange(
    queue.keys.delayed,
    0,
    0,
    "WITHSCORES"
  );
  return score === undefined ? null : Math.floor(Number(score) / 0x1000);
}

export function shouldKeepDraining({
  now,
  deadline,
  nextWorkAt,
  lingerMs,
}: {
  now: number;
  deadline: number;
  nextWorkAt: number | null;
  lingerMs: number;
}): boolean {
  if (now >= deadline || nextWorkAt === null) return false;
  return nextWorkAt <= Math.min(deadline, now + lingerMs);
}

/**
 * The polling safety net the always-on worker runs on an interval: binds
 * next-reel campaigns, then sweeps for comments webhooks missed.
 */
export async function runPollingSweep(): Promise<void> {
  try {
    const attached = await attachPendingNextReels();
    if (attached.bound > 0 || attached.failedAccounts > 0) {
      console.log("[DM Worker] Next-reel attachment:", attached);
    }
    await reconcileComments();
  } catch (error) {
    console.error("[DM Worker] Comment reconciliation failed:", formatError(error));
  }
}

async function heartbeat() {
  try {
    await recordWorkerHeartbeat({ pid: process.pid, hostname: os.hostname() });
  } catch (error) {
    console.error("[DM Worker] Heartbeat failed:", formatError(error));
  }
}

async function acquireLock(token: string, ttlMs: number): Promise<boolean> {
  const result = await getRedisConnection().set(
    DRAIN_LOCK_KEY,
    token,
    "PX",
    ttlMs,
    "NX"
  );
  return result === "OK";
}

async function releaseLock(token: string): Promise<void> {
  await getRedisConnection()
    .eval(RELEASE_LOCK_SCRIPT, 1, DRAIN_LOCK_KEY, token)
    .catch((error) => {
      console.error("[DM Worker] Releasing drain lock failed:", formatError(error));
    });
}

async function maybePoll(): Promise<boolean> {
  const intervalMs = Number(process.env.COMMENT_POLL_INTERVAL_MS ?? 5 * 60_000);
  // Slightly under the interval, so a cron tick that lands a moment early
  // still runs the sweep instead of skipping a whole interval.
  const gateMs = Math.max(1_000, intervalMs - 10_000);
  const opened = await getRedisConnection().set(
    POLL_GATE_KEY,
    String(Date.now()),
    "PX",
    gateMs,
    "NX"
  );
  if (opened !== "OK") return false;
  await runPollingSweep();
  return true;
}

// Runs the worker under the lock until nothing is due inside the window.
async function drainPass({
  deadline,
  lingerMs,
  checkIntervalMs,
}: {
  deadline: number;
  lingerMs: number;
  checkIntervalMs: number;
}): Promise<boolean> {
  const keepGoing = async () =>
    shouldKeepDraining({
      now: Date.now(),
      deadline,
      nextWorkAt: await nextWorkAt(),
      lingerMs,
    });

  if (!(await keepGoing())) return false;

  const worker = createDMWorker();
  let lastBeat = Date.now();
  try {
    do {
      await sleep(checkIntervalMs);
      if (Date.now() - lastBeat >= HEARTBEAT_INTERVAL_MS) {
        lastBeat = Date.now();
        void heartbeat();
      }
    } while (await keepGoing());
  } finally {
    // Waits for jobs in flight; new ones stay queued for the next run.
    await worker.close();
  }
  return true;
}

export async function drainQueue({
  budgetMs,
  lingerMs = DEFAULT_LINGER_MS,
  poll = false,
  checkIntervalMs = DEFAULT_CHECK_INTERVAL_MS,
}: DrainOptions): Promise<DrainResult> {
  const startedAt = Date.now();
  const deadline = startedAt + budgetMs;
  await heartbeat();

  const polled = poll ? await maybePoll() : false;

  let passes = 0;
  let ranWorker = false;
  for (let attempt = 0; attempt < MAX_RELOCK_PASSES; attempt++) {
    if (Date.now() >= deadline) break;
    const token = randomUUID();
    if (!(await acquireLock(token, deadline - Date.now() + CLOSE_GRACE_MS))) {
      // Another run holds the lock and will pick up what is queued.
      if (passes === 0) {
        return { status: "busy", passes, polled, ranMs: Date.now() - startedAt };
      }
      break;
    }
    passes += 1;
    try {
      ranWorker = (await drainPass({ deadline, lingerMs, checkIntervalMs })) || ranWorker;
    } finally {
      await releaseLock(token);
    }
    // Only go round again if something became ready while stopping.
    const next = await nextWorkAt();
    if (next === null || next > Date.now()) break;
  }

  return {
    status: ranWorker ? "drained" : "idle",
    passes,
    polled,
    ranMs: Date.now() - startedAt,
  };
}
