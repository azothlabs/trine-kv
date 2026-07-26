Feature: Durable commit boundaries
  A successful mutation is a durable state transition. Reopen must recover the
  accepted state whether it still resides in the write-ahead log or has moved
  through table maintenance.

  @REQ-DURABLE-001 @REQ-ASYNC-001
  Scenario: A confirmed value survives reopen before flush
    Given a new durable database
    When I write key "session" with value "confirmed"
    And I reopen the database
    And I read key "session"
    Then the value is "confirmed"

  @REQ-DURABLE-002
  Scenario: A confirmed deletion survives reopen
    Given a new durable database
    And key "session" contains "obsolete"
    When I delete key "session"
    And I reopen the database
    Then key "session" is absent

  @REQ-DURABLE-003
  Scenario: Maintenance cannot change the latest accepted value
    Given a new durable database
    And key "item" contains "v1"
    When I flush the database
    And I write key "item" with value "v2"
    And I flush the database
    And I compact the database
    And I reopen the database
    And I read key "item"
    Then the value is "v2"

  @REQ-VERSION-001
  Scenario: An empty batch does not invent a new database state
    Given a new durable database
    And I remember the latest read version
    When I commit an empty batch
    Then the latest read version is unchanged

  @REQ-LIFECYCLE-001
  Scenario: A closed handle fails explicitly
    Given a new durable database
    When I close the database
    And I try to read key "anything"
    Then the operation is rejected because the database is closed

  @REQ-ASYNC-002
  Scenario: An asynchronous mutation has no effect until it is polled
    Given a new durable database
    When I create and discard an unpolled write of key "never-polled" as "invisible"
    And I reopen the database
    Then key "never-polled" is absent
