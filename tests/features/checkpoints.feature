Feature: Durable checkpoint reachability
  A named checkpoint is an application-owned promise that an older committed
  view remains readable until the checkpoint is deleted.

  @REQ-HISTORY-004 @REQ-RECOVERY-003 @REQ-ASYNC-001
  Scenario: A checkpoint preserves the old value after maintenance and reopen
    Given a new durable database
    And key "policy" contains "v1"
    And I create checkpoint "release-1"
    When I write key "policy" with value "v2"
    And I flush the database
    And I compact the database
    And I reopen the database
    And I reopen the database
    And I read key "policy" at the checkpoint
    Then the value is "v1"
    And key "policy" contains "v2"

  @REQ-HISTORY-005
  Scenario: Deleting a checkpoint releases its historical promise
    Given a new durable database retaining only the latest read version
    And key "policy" contains "v1"
    And I create checkpoint "release-1"
    When I write key "policy" with value "v2"
    And I delete checkpoint "release-1"
    And I flush the database
    And I compact the database
    And I try to open the checkpoint read version
    Then the operation is rejected because the read version expired

  @REQ-HISTORY-006
  Scenario: A checkpoint name cannot silently move to another version
    Given a new durable database
    And I create checkpoint "release-1"
    When I write key "policy" with value "v2"
    And I try to create checkpoint "release-1"
    Then the operation is rejected because the checkpoint already exists
