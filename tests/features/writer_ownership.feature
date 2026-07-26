Feature: Durable writer ownership
  A durable database has one active writer. Read-only access may inspect
  committed state but cannot mutate it, and ownership failures are typed.

  @REQ-OWNER-001
  Scenario: A second writer cannot open the same durable database
    Given a new durable database
    And the current writer confirms its durable ownership
    When another writer tries to open the same database
    Then the operation is rejected because the writer lease is unavailable

  @REQ-OWNER-002
  Scenario: A read-only handle can read but cannot write
    Given a new durable database
    And key "policy" contains "published"
    When I reopen the database read-only
    And I read key "policy"
    Then the value is "published"
    When I try to write key "policy" with value "changed"
    Then the operation is rejected because the database is read-only
