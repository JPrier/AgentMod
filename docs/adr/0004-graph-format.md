# ADR 0004: Session graph format

Status: Accepted

Session graphs use versioned TOML. A deterministic compiler produces an inspectable
executable graph and cache key from graph, plugin, runtime API, and capability hashes.
Conditions use AgentMod's bounded expression engine, not arbitrary code.

Compilation rejects missing/unreachable nodes, missing termination, illegal cycles,
unbounded loops, invalid parallel writes, missing capabilities, and budget errors.
TOML is chosen for readable local configuration; wire representations remain separate.
