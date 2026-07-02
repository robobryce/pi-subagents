/**
 * Generic background-work provider contract for `subagent_wait`.
 *
 * `subagent_wait` blocks the current turn until outstanding out-of-band work finishes.
 * Async subagent runs are tracked natively; but other extensions also produce
 * background work the agent may need to join on — e.g. pi-patty-bg-tasks'
 * backgrounded shell/agent jobs, a job-queue extension, a remote-exec worker,
 * a test-runner, etc.
 *
 * Rather than hardcoding one extension's identifiers, `subagent_wait` reads a process-
 * global registry of providers. Any extension in the same Node process can
 * register a provider that answers two questions:
 *
 *   - `liveCount()` — how many of my background units are still in flight?
 *   - `wakeChannels` — which event-bus channels should end subagent_wait's poll-sleep
 *      early because my state just changed?
 *
 * `subagent_wait` sums `liveCount()` across all providers and subscribes to the union of
 * their `wakeChannels`. A provider that isn't installed simply isn't in the
 * registry, so this is null-safe and dependency-free (no imports between the
 * packages — just a shared global keyed by a stable, versioned symbol).
 */

/** A source of background work that `subagent_wait` can join on. */
export interface BackgroundWorkProvider {
	/**
	 * Stable identifier for this provider (e.g. "pi-patty-bg-tasks"). Used to
	 * de-duplicate registrations and for diagnostics. Registering the same name
	 * twice replaces the earlier entry.
	 */
	name: string;
	/**
	 * Count of this provider's background units currently in flight. Must be
	 * cheap and synchronous — `subagent_wait` calls it every poll tick. Return 0 when
	 * nothing is running. Should never throw; throwing providers are ignored.
	 */
	liveCount(): number;
	/**
	 * Event-bus channel names this provider emits when its state changes (e.g. a
	 * job finished). `subagent_wait` subscribes to these so it wakes the instant work
	 * finishes instead of waiting out the poll interval. Optional; a provider
	 * with no push signal still works via the poll fallback.
	 */
	wakeChannels?: readonly string[];
}

/**
 * Versioned global key holding the provider registry. Bumping the suffix is a
 * breaking change to the registry shape; `subagent_wait` and every provider must agree.
 */
const REGISTRY_KEY = "__pi_bg_work_providers_v1";

type Registry = Map<string, BackgroundWorkProvider>;

function registry(): Registry {
	const g = globalThis as Record<string, unknown>;
	let reg = g[REGISTRY_KEY] as Registry | undefined;
	if (!(reg instanceof Map)) {
		reg = new Map<string, BackgroundWorkProvider>();
		g[REGISTRY_KEY] = reg;
	}
	return reg;
}

/**
 * Register (or replace) a background-work provider. Returns an unregister
 * function. Safe to call from any extension; the registry is shared process-
 * wide. Idempotent by `name`.
 */
export function registerBackgroundWorkProvider(provider: BackgroundWorkProvider): () => void {
	if (!provider || typeof provider.liveCount !== "function" || typeof provider.name !== "string") {
		return () => {};
	}
	const reg = registry();
	reg.set(provider.name, provider);
	return () => {
		const current = reg.get(provider.name);
		if (current === provider) reg.delete(provider.name);
	};
}

/** All registered providers (snapshot). */
export function backgroundWorkProviders(): BackgroundWorkProvider[] {
	return [...registry().values()];
}

/** Total in-flight background units across all registered providers. Never throws. */
export function totalLiveBackgroundWork(): number {
	let total = 0;
	for (const p of registry().values()) {
		try {
			const n = p.liveCount();
			if (Number.isFinite(n) && n > 0) total += n;
		} catch {
			// A misbehaving provider must not break subagent_wait's accounting.
		}
	}
	return total;
}

/** Union of wake channels across all registered providers. */
export function backgroundWorkWakeChannels(): string[] {
	const channels = new Set<string>();
	for (const p of registry().values()) {
		for (const ch of p.wakeChannels ?? []) channels.add(ch);
	}
	return [...channels];
}
