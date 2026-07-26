Feature: Durable branch lineages
  A named branch is a persistent copy-on-write lineage. Its parent view is
  frozen at the fork, its divergence is isolated, and its ancestry constrains
  deletion and retained history.

  @REQ-BRANCH-001 @REQ-BRANCH-002
  Scenario: A branch keeps its fork view while root and branch diverge
    Given a new durable native database retaining only the latest read version
    And named bucket "data" contains key "shared" with value "fork"
    And I create durable branch "dev"
    When named bucket "data" writes key "shared" with value "root-later"
    And branch "dev" writes named bucket "data" key "shared" as "branch-later"
    And branch "dev" writes named bucket "data" key "branch-only" as "private"
    Then named bucket "data" key "shared" contains "root-later"
    And key "branch-only" is absent from named bucket "data"
    And branch "dev" key "shared" in named bucket "data" contains "branch-later"
    And branch "dev" key "branch-only" in named bucket "data" contains "private"

  @REQ-BRANCH-003 @REQ-BRANCH-007
  Scenario: Recreating a deleted branch starts a clean durable generation
    Given a new durable native database retaining only the latest read version
    And named bucket "data" contains key "shared" with value "root"
    And I create durable branch "dev"
    When branch "dev" writes named bucket "data" key "shared" as "old-generation"
    And branch "dev" writes named bucket "data" key "old-only" as "old"
    And I delete durable branch "dev"
    And I create durable branch "dev"
    And branch "dev" writes named bucket "data" key "new-only" as "new"
    And I reopen the native database
    Then branch "dev" key "shared" in named bucket "data" contains "root"
    And branch "dev" key "old-only" in named bucket "data" is absent
    And branch "dev" key "new-only" in named bucket "data" contains "new"

  @REQ-BRANCH-004
  Scenario: A branch range orders the winning rows from its whole view
    Given a new durable native database retaining only the latest read version
    And named bucket "data" contains keys "a=one,b=two,c=three"
    And I create durable branch "dev"
    When branch "dev" writes named bucket "data" key "b" as "branch-two"
    And branch "dev" deletes named bucket "data" key "c"
    And branch "dev" writes named bucket "data" key "d" as "four"
    And I scan named bucket "data" on branch "dev"
    Then the branch rows are "a=one,b=branch-two,d=four"

  @REQ-BRANCH-005 @REQ-BRANCH-009
  Scenario: A child branch freezes its parent and reads through to root
    Given a new durable native database retaining only the latest read version
    And named bucket "data" contains key "root-only" with value "root"
    And I create durable branch "parent"
    When branch "parent" writes named bucket "data" key "shared" as "parent-at-fork"
    And I create durable branch "child" from branch "parent"
    And branch "parent" writes named bucket "data" key "shared" as "parent-later"
    Then branch "child" key "shared" in named bucket "data" contains "parent-at-fork"
    And branch "child" key "root-only" in named bucket "data" contains "root"
    And branch "child" reports parent "parent"

  @REQ-BRANCH-006
  Scenario: A parent branch cannot be deleted before its child
    Given a new durable native database retaining only the latest read version
    And I create durable branch "parent"
    And I create durable branch "child" from branch "parent"
    When I try to delete durable branch "parent"
    Then the operation is rejected while the branch has a child
    When I delete durable branch "child"
    And I delete durable branch "parent"
    Then no durable branches are listed

  @REQ-BRANCH-008
  Scenario: A live branch retains its fork through root maintenance and reopen
    Given a new durable native database retaining only the latest read version
    And named bucket "data" contains key "shared" with value "fork"
    And I create durable branch "dev"
    When the root advances key "shared" through 24 later values in named bucket "data"
    And I flush the database
    And I compact the database
    And I reopen the native database
    Then branch "dev" key "shared" in named bucket "data" contains "fork"
