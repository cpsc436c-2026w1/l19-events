@JS03
Feature: A table I query changes
  When a table I query in the lake changes (a few times a month), I want the catalog to show what changed and every
  new column to have a description within a day, so that I do not write a query against a column whose meaning
  changed.

  @R010
  Scenario Outline: A catalog event is recorded without an alert
    Given the simulator delivers the catalog events of the case "<case>"
    When the catalog watch runs on each event
    Then the table "<table>" is recorded
    And 0 alerts are raised

    Examples:
      | case          | table               |
      | new-table     | price_window_counts |
      | new-partition | prices_curated      |

  @R020
  Scenario Outline: A change to a column the job reads blocks the next run
    Given the simulator delivers the catalog events of the case "<case>"
    When the catalog watch runs on each event
    Then the actions include "block_job"
    And 1 alert is raised

    Examples:
      | case           |
      | renamed-column |
      | type-change    |

  @R030
  Scenario: A removed column that the job does not read alerts the owners only
    Given the simulator delivers the catalog events of the case "dropped-column"
    When the catalog watch runs on each event
    Then the actions include "alert_owners"
    And the actions do not include "block_job"
    And 1 alert is raised

  @R040
  Scenario: A new column gets a draft that waits for a person
    Given the simulator delivers the catalog events of the case "new-column"
    And the drafter's model is the stub "stub"
    When the drafter runs on each event
    Then a draft for column "on_promotion" is in the pending folder with status "draft"
    And the draft has at most 255 characters and names the type "boolean"
    And the column "on_promotion" does not have a description in the catalog

  @R040
  Scenario: A draft the check rejects is asked for again with the check's feedback
    Given the simulator delivers the catalog events of the case "new-column"
    And the drafter's model is the stub "stub"
    When the drafter runs on each event
    Then the drafter made 2 model calls
    And the second user message carries the check's feedback

  @R040
  Scenario: The drafter stops after two retries
    Given the simulator delivers the catalog events of the case "new-column"
    And the drafter's model is the stub "stub-stubborn"
    When the drafter runs on each event
    Then the drafter made 3 model calls
    And a draft for column "on_promotion" is in the pending folder with status "cannot_describe"

  @R050
  Scenario: Events that arrive twice and out of order end with the current definition
    Given the simulator delivers the catalog events of the case "repeated-event"
    When the catalog watch runs on each event
    Then the stored version of "prices_raw" is 8
    And 1 alert is raised

  @R060
  Scenario: A change without an event is found by the daily check
    Given the simulator delivers the catalog events of the case "no-event"
    When the catalog watch runs on each event
    And the daily check runs
    Then the actions include "block_job"
    And the removed columns are "currency"
