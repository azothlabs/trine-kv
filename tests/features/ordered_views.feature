Feature: Ordered views
  Range and prefix cursors expose one newest live value per key in bytewise
  order and preserve the view captured when the cursor is created.

  @REQ-CURSOR-001
  Scenario: A forward cursor is sorted and omits deleted keys
    Given a new durable database
    And keys "c=three,a=one,b=two,d=four" exist
    When I delete key "b"
    And I scan all keys forward
    Then the rows are "a=one,c=three,d=four"

  @REQ-CURSOR-002
  Scenario: A reverse cursor returns the same live set in reverse order
    Given a new durable database
    And keys "c=three,a=one,b=two" exist
    When I scan all keys in reverse
    Then the rows are "c=three,b=two,a=one"

  @REQ-CURSOR-003
  Scenario: A bounded cursor honors a half-open end
    Given a new durable database
    And keys "a=one,b=two,c=three,d=four" exist
    When I scan keys from "b" up to "d"
    Then the rows are "b=two,c=three"

  @REQ-CURSOR-004
  Scenario: A prefix cursor returns only matching keys
    Given a new durable database
    And keys "acct:2=Bob,other:1=X,acct:1=Ada,acct2:1=Y" exist
    When I scan keys with prefix "acct:"
    Then the rows are "acct:1=Ada,acct:2=Bob"

  @REQ-CURSOR-005
  Scenario: A cursor keeps its captured view while later writes commit
    Given a new durable database
    And keys "a=one,b=two" exist
    And I create a forward cursor over all keys
    When I write key "c" with value "three"
    And I drain the retained cursor
    Then the rows are "a=one,b=two"
