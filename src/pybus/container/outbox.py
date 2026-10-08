"""The outbox relay. Filled in by the next commit; the producer's
`message.timeout.ms` already has to agree with this constant."""

FLUSH_TIMEOUT_SECONDS = 10.0
