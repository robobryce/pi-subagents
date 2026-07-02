/**
 * Non-interactive auto-drain: block turn-end until outstanding background work
 * finishes when there is no next turn to receive completions.
 *
 * In an interactive session, a detached async run (or a registered background-
 * work provider's job, e.g. pi-patty-bg-tasks) finishing later wakes the parent
 * with a completion notification. Non-interactively (`pi -p ...`) the whole task
 * is a single turn: once the model stops, the process exits and any in-flight
 * work is abandoned — the async runner is orphaned, its result never observed;
 * a patty bash job is killed at shutdown. Either way the model "finished"
 * without the work it started.
 *
 * `subagent_wait` already closes this gap when the model calls it. This closes
 * it when the model DOESN'T: on `agent_end` in a non-interactive session we run
 * the same wait-until-drained logic ourselves, so the turn cannot end while
 * tracked work is still in flight. The model no longer has to remember to call
 * `subagent_wait`.
 *
 * This is gated on non-interactive mode (no UI) and only blocks while there is
 * actually outstanding work, so it is a no-op for ordinary interactive turns and
 * for turns that started nothing.
 */

import { waitForSubagents } from "./subagent-wait.ts";
import { totalLiveBackgroundWork } from "./bg-providers.ts";
import { listAsyncRuns } from "./async-status.ts";
import { ASYNC_DIR, RESULTS_DIR, type SubagentState } from "../../shared/types.ts";
import type { WaitEventBus } from "./subagent-wait.ts";

/** Are there any tracked async runs or provider jobs still in flight for this session? */
function hasOutstandingWork(state: SubagentState): boolean {
	if (totalLiveBackgroundWork() > 0) return true;
	try {
		const active = listAsyncRuns(ASYNC_DIR, {
			states: ["queued", "running"],
			sessionId: state.currentSessionId ?? undefined,
			resultsDir: RESULTS_DIR,
		});
		return active.length > 0;
	} catch {
		return false;
	}
}

export interface AutoDrainDeps {
	state: SubagentState;
	events?: WaitEventBus;
	/** Overall cap so a stuck job can't hang process exit forever. Default 30 min. */
	timeoutMs?: number;
	/** Injectable for tests; defaults to the real wait. */
	wait?: typeof waitForSubagents;
	/** Injectable for tests; defaults to hasOutstandingWork. */
	hasWork?: (state: SubagentState) => boolean;
}

/**
 * Block until every tracked async run and registered background-work provider
 * job for this session is terminal (or the timeout elapses). Safe to call when
 * nothing is outstanding — returns immediately. Never throws.
 *
 * Returns a short human-readable note about what it waited on (or undefined when
 * there was nothing to wait for), for optional logging by the caller.
 */
export async function drainOutstandingWork(deps: AutoDrainDeps): Promise<string | undefined> {
	const hasWork = deps.hasWork ?? hasOutstandingWork;
	if (!hasWork(deps.state)) return undefined;
	const wait = deps.wait ?? waitForSubagents;
	try {
		const res = await wait(
			{ all: true, timeoutMs: deps.timeoutMs },
			undefined,
			{ state: deps.state, events: deps.events },
		);
		const text = res.content.map((c) => ("text" in c ? (c.text ?? "") : "")).join(" ").trim();
		return text || "drained outstanding background work";
	} catch (error) {
		// Draining is best-effort; never block process exit on our own failure.
		return `auto-drain error: ${error instanceof Error ? error.message : String(error)}`;
	}
}
