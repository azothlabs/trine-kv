Feature: Immutable content lifecycle
  Upload bytes are staging data. They become addressable immutable content only
  at seal, and the resulting identity and bytes survive database reopen.

  @REQ-CONTENT-001
  Scenario: Staged bytes are not visible through their future content identity
    Given a new durable database
    When I stage immutable content "unsealed payload" without sealing it
    And I try to open the staged content by its expected identity
    Then the operation is rejected because content is not published

  @REQ-CONTENT-002
  Scenario: Sealed content survives reopen with the same bytes
    Given a new durable database
    When I upload and seal immutable content "immutable payload"
    And I reopen the database
    And I read the sealed content
    Then the value is "immutable payload"

  @REQ-CONTENT-003
  Scenario: Equal bytes in one storage domain have one immutable identity
    Given a new durable database
    When I upload and seal immutable content "deduplicated payload"
    And I upload and seal the same immutable content again
    Then both seals return the same content identity
