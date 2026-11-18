@JS01
Feature: Price updates of my store
  When I change shelf prices in my store during the day (about 60 updates an hour, up to 120 in two minutes when a
  promotion starts), I want to see within 5 minutes how many of my updates arrived and their average price, so that
  I know the new prices are in before customers see them.

  Background:
    Given the simulator delivers one hour of price updates to the counting function

  @R010
  Scenario: One count and one average for each store and window
    Then window 09:00 has 6 updates with an average of 6.95 for store north
    And every window has the count and the average of the simulator's truth

  @R020
  Scenario: An update delivered twice is counted once
    Given the simulator delivers 4 of the updates a second time
    Then window 09:25 has 4 updates for store online
    And the number of updates stored equals the number in the stream

  @R030
  Scenario: A late update goes to the window in which the price changed
    Given the simulator holds the updates of store centre from 09:10 to 09:16
    Then window 09:10 has 4 updates for store centre
    And 4 updates are stored as corrections for window 09:10

  @R040
  Scenario: An update in another currency is kept out of the average
    Given the simulator sends one update of store online in EUR
    Then 1 update is in the rejected list with currency EUR
    And window 09:30 has 4 updates with an average of 6.17 for store online

  @R050
  Scenario: A burst from one store is counted in full and flagged
    Given the simulator sends 120 updates of store north in two minutes from 09:20
    Then window 09:20 has 127 updates for store north
    And the only window flagged as a burst is 09:20 for store north

  @R060
  Scenario: A window without updates is reported with a count of 0
    Given the simulator does not send an update from 09:40 to 09:50
    Then windows 09:40 and 09:45 are reported with a count of 0 for every store

  @R070
  Scenario Outline: An update that does not fit the input model is rejected with the field named
    Given the simulator plants an update with <fault>
    When the counting function receives it
    Then the update is in the rejected list and the reason names the field "<field>"
    And the counting function does not fail

    Examples:
      | fault            | field |
      | a missing price  | price |
      | a price as text  | price |
      | an unknown store | store |

  @R080
  Scenario: One bad record does not stop the other records of its batch (simulated)
    Given the simulator plants a batch of 8 updates whose sixth lacks a price
    And the counting function does not check its input
    When Lambda delivers the batch, splits it on an error and sends a failed record aside
    Then 7 updates are applied once
    And 1 record is in the failure list, with its sequence number and without its content
    And delivering the batch again counts 0 updates twice

  @R080
  Scenario: With the input check the same batch does not fail (simulated)
    Given the simulator plants a batch of 8 updates whose sixth lacks a price
    When Lambda delivers the batch, splits it on an error and sends a failed record aside
    Then 7 updates are applied once
    And 0 records are in the failure list

  @R090
  Scenario: A replay from the start of the stream gives the stored numbers
    When the window numbers are rebuilt from the start of the stream
    Then the replayed numbers equal the stored numbers

  @R090
  Scenario: A replay shows what a function without the duplicate check counted twice
    Given the counting function does not skip an update it has seen
    When the window numbers are rebuilt from the start of the stream
    Then the replayed numbers differ from the stored numbers in window 09:25 only
