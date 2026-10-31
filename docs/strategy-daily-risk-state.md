# Daily strategy risk state

The strategy runtime accepts an optional caller-supplied daily state binding. The
binding identifies the strategy, logical account scope, frozen candidate and
configuration, durable state store, effective session, frozen input digest and
expected predecessor session. A first transition additionally requires a
bootstrap binding independently authorized by the caller. This interface does
not derive these values from runtime configuration or establish authorization.

The runtime reads and validates the scoped chain and confirms a proposed state
transition before using the persisted head. Missing, malformed, conflicting or
unavailable state cannot authorize a new risk increase. Without an execution
fence that binds the state and intent to broker submission, state enrollment
preserves independently valid reductions and does not authorize risk increases,
even after the state transition is persisted. Default runtime configuration does
not enroll this binding. Configurations using a distinct volatility exit
threshold and partial retention require durable state enrollment before relying
on stateful increases; no migration is implied by this interface.

Each cycle uses one UTC whole-second decision clock for supported plugin and
risk-gate checks. The trading session remains a separate value. Independently
supplied expected artifact digests are matched to exact references; absent
bindings remain unverified. The decision receipt records actual pre- and
post-gate target digests when the shared runtime supports them, and a separate
partial link records an actual execution receipt when available. An artifact
read time does not prove that it was published or available before the decision.
Receipt observation failures identify the source, candidate write, archive
read, projection, coverage, archive write or recovery stage without exposing
private source paths. A healthy receipt keeps its original business-run identity.
