Feature: Durable content read authority
  Immutable bytes remain readable only through the authority established for
  their storage domain. Leases and physical holds retain exact durable
  identities; the leased-only boundary is one-way.

  @REQ-ACCESS-001 @REQ-ACCESS-002 @REQ-ACCESS-003 @REQ-ACCESS-004 @REQ-ACCESS-005
  Scenario: Leased-only enforcement fences new ordinary opens without closing old handles
    Given a new durable database
    And sealed content contains "barrier bytes"
    And I retain an ordinary handle to the sealed content
    When I enforce leased-only access for the content domain
    And I repeat leased-only enforcement for the content domain
    Then both enforcement calls report the same barrier
    When I try to open the sealed content without a lease
    Then the operation is rejected because a content lease is required
    And the retained pre-barrier content handle reads "barrier bytes"
    When I open the sealed content with a 60 second lease
    Then the leased content handle reads "barrier bytes"
    When I reopen the database
    And I try to open the sealed content without a lease
    Then the operation is rejected because a content lease is required

  @REQ-LEASE-001 @REQ-LEASE-002
  Scenario: Cloned content handles share a renewed durable lease
    Given a new durable database
    And sealed content contains "leased bytes"
    When I open the sealed content with a 60 second lease
    And I clone the leased content handle
    And I remember the leased content deadline
    And I renew the cloned content lease for 120 seconds
    Then both leased handles report a later common deadline
    And the leased content handle reads "leased bytes"

  @REQ-LEASE-003
  Scenario: An expired content lease cannot read or renew
    Given a new durable database
    And sealed content contains "short lease"
    When I open the sealed content with a 5 millisecond lease
    And I wait until the lease deadline has passed
    And I try to read through the leased content handle
    Then the operation is rejected because the content lease expired
    When I try to renew the leased content handle
    Then the operation is rejected because the content lease expired

  @REQ-LEASE-004
  Scenario: A read-only database cannot publish a content lease
    Given a new durable database
    And sealed content contains "read only bytes"
    When I reopen the database read-only
    And I try to open the sealed content with a lease
    Then the operation is rejected because the database is read-only

  @REQ-HOLD-001 @REQ-HOLD-002
  Scenario: An until-released physical hold resumes after reopen and stays released
    Given a new durable database
    And sealed content contains "held bytes"
    When I acquire a remembered until-released backup hold
    And I reopen the database
    And I resume the remembered physical hold
    And I release the remembered physical hold twice
    And I reopen the database
    And I try to resume the remembered physical hold
    Then the operation is rejected because the physical hold is absent

  @REQ-HOLD-003
  Scenario: A different owner cannot resume a physical hold
    Given a new durable database
    And sealed content contains "owned hold"
    When I acquire a remembered until-released backup hold
    And I try to resume the remembered physical hold as another owner
    Then the operation is rejected because the physical hold owner differs

  @REQ-HOLD-004
  Scenario: An expiring physical hold never shortens and cannot be revived
    Given a new durable database
    And sealed content contains "expiring hold"
    When I acquire a remembered expiring provider hold for 60 seconds
    And I remember the physical hold deadline
    And I renew the physical hold for 1 millisecond
    Then the physical hold deadline is unchanged
    When I renew the physical hold for 120 seconds
    Then the physical hold deadline is later
    When I acquire a second expiring provider hold for 5 milliseconds
    And I wait until the physical hold deadline has passed
    And I try to renew the physical hold for 2 seconds
    Then the operation is rejected because the physical hold expired
    When I reopen the database
    And I try to resume the remembered physical hold
    Then the operation is rejected because the physical hold expired

  @REQ-HOLD-005
  Scenario Outline: Every physical hold class retains the same exact content
    Given a new durable database
    And sealed content contains "class held bytes"
    When I acquire a remembered until-released "<kind>" hold
    And I reopen the database
    And I resume the remembered physical hold
    Then the resumed physical hold class is "<kind>"

    Examples:
      | kind           |
      | migration      |
      | backup         |
      | repair         |
      | provider       |
      | administrative |
      | processing     |
      | offline        |
