import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const {
  mockRedis,
  mockQueue,
  mockWorker,
  mockCreateDMWorker,
  mockRecordHeartbeat,
  mockReconcile,
  mockAttach,
} = vi.hoisted(() => {
  const mockWorker = { close: vi.fn() };
  return {
    mockRedis: { set: vi.fn(), eval: vi.fn(), zrange: vi.fn() },
    mockQueue: {
      getJobCounts: vi.fn(),
      keys: { delayed: "bull:dm-processing:delayed" },
    },
    mockWorker,
    mockCreateDMWorker: vi.fn(() => mockWorker),
    mockRecordHeartbeat: vi.fn(),
    mockReconcile: vi.fn(),
    mockAttach: vi.fn(),
  };
});

vi.mock("@/lib/queue/client", () => ({
  getDMQueue: () => mockQueue,
  getRedisConnection: () => mockRedis,
}));
vi.mock("@/lib/queue/dm-worker", () => ({ createDMWorker: mockCreateDMWorker }));
vi.mock("@/lib/ops/worker-health", () => ({
  recordWorkerHeartbeat: mockRecordHeartbeat,
}));
vi.mock("@/lib/polling/comment-reconciler", () => ({
  reconcileComments: mockReconcile,
}));
vi.mock("@/lib/automation/attach-next-reel", () => ({
  attachPendingNextReels: mockAttach,
}));

import {
  drainQueue,
  isInlineWorkerEnabled,
  nextWorkAt,
  shouldKeepDraining,
} from "@/lib/queue/drain";

const idleCounts = { waiting: 0, active: 0, prioritized: 0 };

describe("shouldKeepDraining", () => {
  const now = 1_000_000;
  const base = { now, deadline: now + 100_000, lingerMs: 60_000 };

  it("keeps going while a job is ready", () => {
    expect(shouldKeepDraining({ ...base, nextWorkAt: now })).toBe(true);
  });

  it("waits for a delayed job due inside the linger window", () => {
    expect(shouldKeepDraining({ ...base, nextWorkAt: now + 40_000 })).toBe(true);
  });

  it("leaves a job due after the linger window to the next tick", () => {
    expect(shouldKeepDraining({ ...base, nextWorkAt: now + 61_000 })).toBe(false);
  });

  it("does not wait for a job due after the deadline", () => {
    expect(
      shouldKeepDraining({ ...base, deadline: now + 10_000, nextWorkAt: now + 20_000 })
    ).toBe(false);
  });

  it("stops at the deadline even with work ready", () => {
    expect(
      shouldKeepDraining({ ...base, deadline: now, nextWorkAt: now })
    ).toBe(false);
  });

  it("stops on an empty queue", () => {
    expect(shouldKeepDraining({ ...base, nextWorkAt: null })).toBe(false);
  });
});

describe("nextWorkAt", () => {
  beforeEach(() => vi.clearAllMocks());

  it("is now when jobs are waiting", async () => {
    mockQueue.getJobCounts.mockResolvedValue({ ...idleCounts, waiting: 2 });
    expect(await nextWorkAt(123)).toBe(123);
    expect(mockRedis.zrange).not.toHaveBeenCalled();
  });

  it("counts active jobs, so a run recovers one a killed function left behind", async () => {
    mockQueue.getJobCounts.mockResolvedValue({ ...idleCounts, active: 1 });
    expect(await nextWorkAt(123)).toBe(123);
  });

  it("decodes the earliest delayed job's due time from its score", async () => {
    mockQueue.getJobCounts.mockResolvedValue(idleCounts);
    const due = 1_800_000_000_000;
    mockRedis.zrange.mockResolvedValue(["job-1", String(due * 0x1000 + 7)]);
    expect(await nextWorkAt()).toBe(due);
  });

  it("is null when the queue is empty", async () => {
    mockQueue.getJobCounts.mockResolvedValue(idleCounts);
    mockRedis.zrange.mockResolvedValue([]);
    expect(await nextWorkAt()).toBeNull();
  });
});

describe("drainQueue", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockRedis.set.mockResolvedValue("OK");
    mockRedis.eval.mockResolvedValue(1);
    mockRedis.zrange.mockResolvedValue([]);
    mockWorker.close.mockResolvedValue(undefined);
    mockAttach.mockResolvedValue({ bound: 0, failedAccounts: 0 });
  });

  it("does not start a worker when nothing is due", async () => {
    mockQueue.getJobCounts.mockResolvedValue(idleCounts);
    const result = await drainQueue({ budgetMs: 10_000, checkIntervalMs: 1 });
    expect(result.status).toBe("idle");
    expect(mockCreateDMWorker).not.toHaveBeenCalled();
    expect(mockRecordHeartbeat).toHaveBeenCalled();
    // The lock is released.
    expect(mockRedis.eval).toHaveBeenCalledTimes(1);
  });

  it("runs the worker until the queue is empty, then closes it", async () => {
    mockQueue.getJobCounts
      .mockResolvedValueOnce({ ...idleCounts, waiting: 3 })
      .mockResolvedValueOnce({ ...idleCounts, active: 1 })
      .mockResolvedValue(idleCounts);
    const result = await drainQueue({ budgetMs: 10_000, checkIntervalMs: 1 });
    expect(result.status).toBe("drained");
    expect(mockCreateDMWorker).toHaveBeenCalledTimes(1);
    expect(mockWorker.close).toHaveBeenCalledTimes(1);
    expect(mockRedis.eval).toHaveBeenCalledTimes(1);
  });

  it("closes the worker and releases the lock at the deadline", async () => {
    mockQueue.getJobCounts.mockResolvedValue({ ...idleCounts, waiting: 1 });
    const result = await drainQueue({ budgetMs: 30, checkIntervalMs: 5 });
    expect(result.status).toBe("drained");
    expect(mockWorker.close).toHaveBeenCalledTimes(1);
    expect(mockRedis.eval).toHaveBeenCalled();
  });

  it("leaves the queue to the run holding the lock", async () => {
    mockRedis.set.mockResolvedValue(null);
    mockQueue.getJobCounts.mockResolvedValue({ ...idleCounts, waiting: 1 });
    const result = await drainQueue({ budgetMs: 10_000, checkIntervalMs: 1 });
    expect(result.status).toBe("busy");
    expect(mockCreateDMWorker).not.toHaveBeenCalled();
    expect(mockRedis.eval).not.toHaveBeenCalled();
  });

  it("goes round again when a job arrives while it was stopping", async () => {
    mockQueue.getJobCounts
      .mockResolvedValueOnce(idleCounts) // pass 1: nothing due
      .mockResolvedValueOnce({ ...idleCounts, waiting: 1 }) // arrived on release
      .mockResolvedValueOnce({ ...idleCounts, waiting: 1 }) // pass 2 starts
      .mockResolvedValue(idleCounts);
    const result = await drainQueue({ budgetMs: 10_000, checkIntervalMs: 1 });
    expect(result.passes).toBe(2);
    expect(mockCreateDMWorker).toHaveBeenCalledTimes(1);
  });

  it("runs the comment sweep only when the poll gate opens", async () => {
    mockQueue.getJobCounts.mockResolvedValue(idleCounts);
    mockRedis.set
      .mockResolvedValueOnce("OK") // poll gate
      .mockResolvedValue("OK");
    const first = await drainQueue({ budgetMs: 1_000, poll: true, checkIntervalMs: 1 });
    expect(first.polled).toBe(true);
    expect(mockAttach).toHaveBeenCalledTimes(1);
    expect(mockReconcile).toHaveBeenCalledTimes(1);

    mockRedis.set.mockReset();
    mockRedis.set
      .mockResolvedValueOnce(null) // gate still closed
      .mockResolvedValue("OK");
    const second = await drainQueue({ budgetMs: 1_000, poll: true, checkIntervalMs: 1 });
    expect(second.polled).toBe(false);
    expect(mockReconcile).toHaveBeenCalledTimes(1);
  });
});

describe("isInlineWorkerEnabled", () => {
  const saved = { ...process.env };
  afterEach(() => {
    process.env = { ...saved };
  });

  it("is on for Vercel deploys by default", () => {
    delete process.env.INLINE_WORKER;
    process.env.VERCEL = "1";
    expect(isInlineWorkerEnabled()).toBe(true);
  });

  it("is off elsewhere by default", () => {
    delete process.env.INLINE_WORKER;
    delete process.env.VERCEL;
    expect(isInlineWorkerEnabled()).toBe(false);
  });

  it("follows INLINE_WORKER when set", () => {
    process.env.VERCEL = "1";
    process.env.INLINE_WORKER = "false";
    expect(isInlineWorkerEnabled()).toBe(false);
    delete process.env.VERCEL;
    process.env.INLINE_WORKER = "true";
    expect(isInlineWorkerEnabled()).toBe(true);
  });
});
