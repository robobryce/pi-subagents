/**
 * Generic background-work provider contract (bg-providers.ts) + its integration
 * with subagent_wait. Deterministic: no real subagents, no real bash. Providers are
 * registered on the shared process-global registry and asserted through both the
 * low-level helpers and an end-to-end subagent_wait() using the real (non-injected)
 * liveBgTaskCount path plus the union of provider wake channels.
 */

import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, describe, it } from "node:test";
import {
	registerBackgroundWorkProvider,
	backgroundWorkProviders,
	totalLiveBackgroundWork,
	backgroundWorkWakeChannels,
} from "../../src/runs/background/bg-providers.ts";
import { waitForSubagents, type SubagentWaitDeps } from "../../src/runs/background/subagent-wait.ts";
import type { SubagentState } from "../../src/shared/types.ts";

const REGISTRY_KEY = "__pi_bg_work_providers_v1";

function resetRegistry() {
	const g = globalThis as Record<string, unknown>;
	delete g[REGISTRY_KEY];
}

function makeState(sessionId: string | null): SubagentState {
	return {
		baseCwd: "", currentSessionId: sessionId, asyncJobs: new Map(),
		foregroundControls: new Map(), lastForegroundControlId: null,
		cleanupTimers: new Map(), lastUiContext: null, poller: null,
		completionSeen: new Map(), watcher: null, watcherRestartTimer: null,
		resultFileCoalescer: { schedule: () => false, clear: () => {} },
	} as SubagentState;
}

function textOf(r: { content: Array<{ type: string; text?: string }> }): string {
	return r.content.map((c) => c.text ?? "").join("");
}

function fakeBus() {
	const handlers = new Map<string, Array<(d: unknown) => void>>();
	return {
		on(ch: string, h: (d: unknown) => void) {
			const l = handlers.get(ch) ?? []; l.push(h); handlers.set(ch, l);
			return () => handlers.set(ch, (handlers.get(ch) ?? []).filter((x) => x !== h));
		},
		emit(ch: string, d: unknown) { for (const h of [...(handlers.get(ch) ?? [])]) h(d); },
	};
}

function emptyRunsDeps(root: string, state: SubagentState, overrides: Partial<SubagentWaitDeps>): SubagentWaitDeps {
	return {
		state,
		asyncDirRoot: path.join(root, "runs"),
		resultsDir: path.join(root, "results"),
		kill: () => true,
		pollIntervalMs: 5,
		...overrides,
	};
}

describe("background-work provider registry", () => {
	afterEach(resetRegistry);

	it("sums liveCount across multiple registered providers", () => {
		resetRegistry();
		registerBackgroundWorkProvider({ name: "a", liveCount: () => 2 });
		registerBackgroundWorkProvider({ name: "b", liveCount: () => 3 });
		assert.equal(totalLiveBackgroundWork(), 5);
	});

	it("unregister removes a provider", () => {
		resetRegistry();
		const off = registerBackgroundWorkProvider({ name: "a", liveCount: () => 4 });
		assert.equal(totalLiveBackgroundWork(), 4);
		off();
		assert.equal(totalLiveBackgroundWork(), 0);
	});

	it("registering the same name replaces the earlier provider", () => {
		resetRegistry();
		registerBackgroundWorkProvider({ name: "dup", liveCount: () => 1 });
		registerBackgroundWorkProvider({ name: "dup", liveCount: () => 9 });
		assert.equal(totalLiveBackgroundWork(), 9);
		assert.equal(backgroundWorkProviders().filter((p) => p.name === "dup").length, 1);
	});

	it("a throwing provider is ignored, not fatal", () => {
		resetRegistry();
		registerBackgroundWorkProvider({ name: "boom", liveCount: () => { throw new Error("nope"); } });
		registerBackgroundWorkProvider({ name: "ok", liveCount: () => 2 });
		assert.equal(totalLiveBackgroundWork(), 2);
	});

	it("unions wake channels across providers (deduped)", () => {
		resetRegistry();
		registerBackgroundWorkProvider({ name: "a", liveCount: () => 0, wakeChannels: ["x", "shared"] });
		registerBackgroundWorkProvider({ name: "b", liveCount: () => 0, wakeChannels: ["y", "shared"] });
		const chans = backgroundWorkWakeChannels().sort();
		assert.deepEqual(chans, ["shared", "x", "y"]);
	});

	it("no providers registered → zero work, no channels", () => {
		resetRegistry();
		assert.equal(totalLiveBackgroundWork(), 0);
		assert.deepEqual(backgroundWorkWakeChannels(), []);
	});
});

describe("subagent_wait × generic providers (no injected bgTaskCount)", () => {
	afterEach(resetRegistry);

	it("blocks on a registered provider's live work and returns when it drains", async () => {
		resetRegistry();
		let live = 2;
		registerBackgroundWorkProvider({ name: "queue", liveCount: () => live });
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-bgp-drain-"));
		try {
			let polls = 0;
			const sleep = async () => { polls += 1; if (polls === 1) live = 1; };
			const r = await waitForSubagents({}, undefined, emptyRunsDeps(root, makeState("s1"), { sleep }));
			assert.equal(r.isError, undefined);
			assert.match(textOf(r), /1 of 2 background job\(s\) finished/);
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("wakes on a custom provider wake channel", async () => {
		resetRegistry();
		let live = 1;
		registerBackgroundWorkProvider({ name: "queue", liveCount: () => live, wakeChannels: ["queue:job-done"] });
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-bgp-wake-"));
		try {
			const bus = fakeBus();
			const realSleep = (ms: number, signal?: AbortSignal) => new Promise<void>((res) => {
				const t = setTimeout(res, ms); signal?.addEventListener("abort", () => { clearTimeout(t); res(); }, { once: true });
			});
			const startedAt = Date.now();
			const p = waitForSubagents({ all: true }, undefined,
				emptyRunsDeps(root, makeState("s1"), { events: bus, pollIntervalMs: 10_000, sleep: realSleep }));
			setTimeout(() => { live = 0; bus.emit("queue:job-done", { id: "q1" }); }, 15);
			const r = await p;
			const elapsed = Date.now() - startedAt;
			assert.equal(r.isError, undefined);
			assert.match(textOf(r), /1 of 1 background job\(s\) finished\./);
			assert.ok(elapsed < 2000, `should wake via custom channel (~15ms), not the 10s poll; took ${elapsed}ms`);
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});
});
