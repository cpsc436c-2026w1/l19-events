@JS02
Feature: The night's export files
  When the three stores' export files land in the raw zone before the 02:00 job (three files a night), I want to
  know before the job starts whether each file is safe to load, and what is wrong with one that is not, so that I
  hold one file at night and do not repair prices_curated in the morning.

  @R010
  Scenario: A normal night passes without a model call
    Given the simulator delivers the export files of the night "normal-day"
    When the file check runs on each new file
    Then 3 files pass and 0 files are held
    And the agent makes 0 model calls

  @R020
  Scenario: A file with repeated ids is held
    Given the simulator delivers the export files of the night "duplicate-rows"
    When the file check runs on each new file
    Then the file of store centre has the verdict "hold" and the cause "duplicate_rows"
    And the check reports 493 repeated ids for store centre

  @R030
  Scenario Outline: A store without a file at 01:55 is named
    Given the simulator delivers the export files of the night "<night>"
    When the check for missing files runs at 01:55
    Then the stores named as missing are "<missing>"

    Examples:
      | night         | missing |
      | missing-store | online  |
      | late-file     | centre  |
      | normal-day    |         |

  @R040
  Scenario: A file whose prices are in another currency than its label is held
    Given the simulator delivers the export files of the night "not-converted"
    When the file check runs on each new file
    Then the file of store online has the verdict "hold" and the cause "currency_not_converted"

  @R050
  Scenario Outline: A file with an unusual row count is held
    Given the simulator delivers the export files of the night "<night>"
    When the file check runs on each new file
    Then the file of store <store> is held
    And 2 files pass

    Examples:
      | night            | store  |
      | half-file        | centre |
      | duplicate-rows   | centre |
      | category-missing | north  |

  @R060
  Scenario Outline: The agent names the cause of a held file and code confirms it
    Given the simulator delivers the export files of the night "<night>"
    And the agent's model is the stub "stub"
    When the agent triages the held file of store <store>
    Then the cause is "<cause>" and code confirms it
    And the proposed fix is "ask_upstream"
    And the agent made at most 12 model calls and 10 tool calls

    Examples:
      | night            | store  | cause            |
      | half-file        | centre | partial_file     |
      | category-missing | north  | category_missing |

  @R060 @limits
  Scenario: A model that never stops is ended by the limits
    Given the simulator delivers the export files of the night "half-file"
    And the agent's model is the stub "stub-runaway"
    When the agent triages the held file of store centre
    Then the agent made at most 12 model calls and 10 tool calls
    And the run was stopped by the app
    And the proposed fix is "escalate"

  @R060 @limits
  Scenario: A cause the data does not show is refused
    Given the simulator delivers the export files of the night "half-file"
    And the agent's model is the stub "stub-wrong"
    When the agent triages the held file of store centre
    Then the cause "late_arrival" is not confirmed by code
    And the proposed fix is "escalate"

  @R060 @limits
  Scenario: A tool refuses a day outside the 8 days it serves
    Given the simulator delivers the export files of the night "category-missing"
    And the agent's model is the stub "stub-outside"
    When the agent triages the held file of store north
    Then the first tool call was refused with an error
    And the cause is "category_missing" and code confirms it

  @R060 @limits
  Scenario: The agent does not run for a file that code settles
    Given the simulator delivers the export files of the night "late-file"
    And the agent's model is the stub "stub-runaway"
    When the agent triages the held file of store centre
    Then the agent made at most 0 model calls and 0 tool calls

  @R080
  Scenario: A file with a row that does not fit the row model is held with the line and the field
    Given the simulator delivers the export files of the night "normal-day"
    And the price on line 4 of the file of store north is "n/a"
    When the file check runs on each new file
    Then the file of store north is held
    And the reason names line 4 and the field "price"

  @R070
  Scenario: A file that arrives after the job started gets one proposed rerun
    Given the simulator delivers the export files of the night "late-file"
    When the file check runs on each new file
    Then the file of store centre has the verdict "rerun_needed" and the cause "late_arrival"
    And the fix "rerun_partition" waits for approval
