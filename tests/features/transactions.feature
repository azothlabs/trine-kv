Feature: Serializable optimistic transactions
  A transaction either commits all staged changes against the read set it
  observed, or reports a conflict and publishes none of them.

  @REQ-TXN-001
  Scenario: A changed point dependency rejects every staged write
    Given a new durable database
    And key "guard" contains "before"
    When a transaction reads "guard", stages key "result" as "accepted", and another writer changes "guard"
    Then the transaction is rejected as a conflict
    And key "result" is absent

  @REQ-TXN-002
  Scenario: A new key inside a read range rejects every staged write
    Given a new durable database
    And keys "a=one,c=three" exist
    When a transaction reads keys from "a" up to "d", stages key "result" as "accepted", and another writer inserts "b"
    Then the transaction is rejected as a conflict
    And key "result" is absent

  @REQ-TXN-003
  Scenario: A successful transaction publishes all staged writes together
    Given a new durable database
    When a transaction stages "a=one,b=two" and commits
    Then key "a" contains "one"
    And key "b" contains "two"
