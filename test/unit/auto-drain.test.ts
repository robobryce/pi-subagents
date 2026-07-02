/**
 * drainOutstandingWork: blocks turn-end until tracked work drains, no-op when
 * nothing outstanding. Deterministic — injects hasWork + a fake wait.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { drainOutstandingWork } from "../../src/runs/background/auto-drain.ts";
import type { SubagentState } from "../../src/shared/types.ts";

function makeState(): SubagentState {
	return { currentSessionId: "s1" } as SubagentState;
}
const okResult = {
	content: [{ type: "text", text: "Waited; done." }],
	details: { mode: "management", results: [] },
} as never;

describe("drainOutstandingWork", () => {
	it("no-op when there is no outstanding work (wait never called)", async () => {
		let called = false;
		const note = await drainOutstandingWork({
			state: makeState(),
			hasWork: () => false,
			wait: (async () => { called = true; return okResult; }) as never,
		});
		assert.equal(called, false, "wait must not be called when nothing is outstanding");
		assert.equal(note, undefined);
	});

	it("calls wait({all:true}) when work is outstanding and returns its note", async () => {
		let params: unknown;
		const note = await drainOutstandingWork({
			state: makeState(),
			hasWork: () => true,
			wait: (async (p: unknown) => { params = p; return okResult; }) as never,
		});
		assert.deepEqual((params as { all?: boolean }).all, true, "must wait for ALL work");
		assert.match(note ?? "", /done/);
	});

	it("never throws if wait rejects (best-effort at exit)", async () => {
		const note = await drainOutstandingWork({
			state: makeState(),
			hasWork: () => true,
			wait: (async () => { throw new Error("boom"); }) as never,
		});
		assert.match(note ?? "", /auto-drain error: boom/);
	});

	it("forwards a timeout cap to wait", async () => {
		let params: { timeoutMs?: number } | undefined;
		await drainOutstandingWork({
			state: makeState(),
			hasWork: () => true,
			timeoutMs: 12345,
			wait: (async (p: { timeoutMs?: number }) => { params = p; return okResult; }) as never,
		});
		assert.equal(params?.timeoutMs, 12345);
	});
});
