Feature: Durable namespace boundaries
  Named buckets are durable, isolated namespaces. A bucket name may be reused
  after deletion, but a handle from the deleted generation must never gain
  authority over the replacement.

  @REQ-NS-001
  Scenario: Equal keys in different namespaces remain isolated after reopen
    Given a new durable database
    And key "identity" contains "default"
    And named bucket "accounts" contains key "identity" with value "account"
    When I reopen the database
    And I read key "identity" from named bucket "accounts"
    Then the value is "account"
    And key "identity" contains "default"

  @REQ-NS-002
  Scenario: An old handle cannot cross a delete and recreate boundary
    Given a new durable database
    And named bucket "sessions" contains key "token" with value "old"
    And I retain a handle to named bucket "sessions"
    When I drop and recreate named bucket "sessions"
    And the retained bucket handle reads key "token"
    Then the retained bucket handle is rejected as stale
    And key "token" is absent from named bucket "sessions"

  @REQ-NS-003
  Scenario: The reserved default namespace cannot be opened as a named bucket
    Given a new durable database
    When I try to open named bucket "default"
    Then the operation is rejected as invalid options

  @REQ-BATCH-002 @REQ-RECOVERY-001 @REQ-ASYNC-001
  Scenario: One accepted batch changes the default and a named namespace together
    Given a new durable database
    And named bucket "ledger" exists
    When I atomically write key "balance" as "90" and named bucket "ledger" key "entry" as "debit-10"
    And I reopen the database
    Then key "balance" contains "90"
    And named bucket "ledger" key "entry" contains "debit-10"

  @REQ-BATCH-001 @REQ-RECOVERY-002
  Scenario: A rejected cross-namespace batch changes no namespace
    Given a new durable database
    And key "balance" contains "100"
    When I try to atomically write key "balance" as "0" and missing bucket "missing" key "entry" as "debit-100"
    Then the operation is rejected because the bucket is missing
    When I reopen the database
    Then key "balance" contains "100"
