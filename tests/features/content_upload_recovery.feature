Feature: Durable content upload recovery
  Upload identity names a durable lifecycle. Confirmed staged bytes resume at
  their exact boundary, while sealing and abort permanently retire writability.

  @REQ-UPLOAD-001
  Scenario: An open upload resumes after reopen and seals the complete byte stream
    Given a new durable database
    When I begin a remembered content upload with bytes "confirmed prefix"
    And I reopen the database
    And I resume the remembered upload and append " and suffix"
    And I seal the remembered upload
    And I read the sealed content
    Then the value is "confirmed prefix and suffix"

  @REQ-UPLOAD-002
  Scenario: A sealed upload resumes as the identical result after reopen
    Given a new durable database
    When I begin a remembered content upload with bytes "stable seal"
    And I seal the remembered upload
    And I remember the sealed content identity
    And I reopen the database
    And I resume the remembered sealed upload
    Then the resumed seal has the remembered content identity

  @REQ-UPLOAD-003
  Scenario: An aborted upload remains retired and never publishes its future identity
    Given a new durable database
    When I begin a remembered content upload with bytes "discarded bytes"
    And I abort the remembered upload
    And I reopen the database
    And I try to resume the remembered upload
    Then the operation is rejected because the content upload is absent
    When I try to open the remembered upload bytes by content identity
    Then the operation is rejected because content is not published

  @REQ-UPLOAD-004
  Scenario: A wrong expected length cannot publish content
    Given a new durable database
    When I begin a content upload expecting 3 bytes
    And I try to write "four" to the upload
    Then the operation is rejected because content length differs
    When I try to open bytes "four" by content identity
    Then the operation is rejected because content is not published

  @REQ-UPLOAD-005
  Scenario: Aborting an upload releases its durable physical reservation
    Given a new durable database
    And the content domain has a physical quota of 5 bytes
    When I begin a remembered content upload expecting 5 bytes
    And I write "12345" to the remembered upload
    And I try to begin another content upload expecting 1 byte
    Then the operation is rejected because the physical content quota is exhausted
    When I abort the remembered upload
    And I begin another content upload expecting 1 byte
    And I write "x" to the upload
    And I seal the upload
    Then the content domain accounts for 1 unique byte and 0 reserved bytes

  @REQ-UPLOAD-006
  Scenario: Open-upload maintenance honors its exclusive inactivity boundary
    Given a new durable database
    When I begin a remembered content upload with bytes "unfinished"
    And I remember the upload maintenance timestamp
    And I abandon the live upload handle without aborting
    And I reap uploads at the exact remembered timestamp
    Then maintenance scanned 1 upload and aborted 0 uploads
    When I prune sealed uploads after the remembered timestamp
    Then maintenance scanned 1 upload and pruned 0 sealed uploads
    When I resume the remembered upload without appending
    Then the resumed upload length is 10 bytes
    When I abandon the live upload handle without aborting
    And I reap uploads after the remembered timestamp
    Then maintenance scanned 1 upload and aborted 1 upload
    When I reopen the database
    And I try to resume the remembered upload
    Then the operation is rejected because the content upload is absent

  @REQ-UPLOAD-006
  Scenario: Sealed-upload maintenance retires only old idempotency state
    Given a new durable database
    When I begin a remembered content upload with bytes "immutable after pruning"
    And I seal the remembered upload
    And I remember the sealed content identity
    And I remember the upload maintenance timestamp
    And I prune sealed uploads at the exact remembered timestamp
    Then maintenance scanned 1 upload and pruned 0 sealed uploads
    When I resume the remembered sealed upload
    Then the resumed seal has the remembered content identity
    When I prune sealed uploads after the remembered timestamp
    Then maintenance scanned 1 upload and pruned 1 sealed upload
    When I reopen the database
    And I try to resume the remembered upload
    Then the operation is rejected because the content upload is sealed
    When I read the sealed content
    Then the value is "immutable after pruning"
