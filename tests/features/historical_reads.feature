Feature: Historical read promises
  Snapshots and accepted read versions are repeatable views. Maintenance may
  reclaim history only after no configured or named promise retains it.

  @REQ-HISTORY-001 @REQ-ASYNC-001
  Scenario: A snapshot remains repeatable across writes and maintenance
    Given a new durable database
    And key "account" contains "v1"
    And I retain a snapshot
    When I write key "account" with value "v2"
    And I flush the database
    And I compact the database
    And I read key "account" from the snapshot
    Then the value is "v1"

  @REQ-HISTORY-002
  Scenario: A future read version is rejected instead of reading latest
    Given a new durable database
    When I try to open a read version newer than the latest
    Then the operation is rejected because the read version is too new

  @REQ-HISTORY-003
  Scenario: Reclaimed history is rejected instead of reading latest
    Given a new durable database retaining only the latest read version
    And key "account" contains "v1"
    And I remember the latest read version
    When I write key "account" with value "v2"
    And I flush the database
    And I compact the database
    And I try to open the remembered read version
    Then the operation is rejected because the read version expired

  @REQ-RANGE-001 @REQ-ASYNC-001
  Scenario: A half-open deletion preserves its end key
    Given a new durable database
    And key "a" contains "one"
    And key "b" contains "two"
    And key "c" contains "three"
    When I delete keys from "a" up to "c"
    And I flush the database
    And I compact the database
    Then key "a" is absent
    And key "b" is absent
    And key "c" contains "three"
