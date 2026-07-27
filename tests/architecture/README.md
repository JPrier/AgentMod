# Architecture validation fixtures

`intentional-violations` is a Cargo workspace that is valid enough for
`cargo metadata` but deliberately violates AgentMod's architectural rules.
The `xtask` test suite asserts the stable diagnostic code for every class of
violation. The fixture crates are not production workspace members and are
not expected to compile.
