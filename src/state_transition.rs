/// Result of validating a requested durable state transition against the
/// currently stored record.
///
/// Domain records decide whether the transition is legal and whether the
/// stored state already represents the requested outcome. Callers only perform
/// irreversible work for `Apply`; `AlreadyApplied` makes recovery idempotent
/// without treating an independently completed operation as corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurableTransition<T> {
    Apply(T),
    AlreadyApplied(T),
}
