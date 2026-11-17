@JS01
Feature: Price updates of my store
  When I change shelf prices in my store during the day (about 60 updates an hour, up to 120 in two minutes when a
  promotion starts), I want to see within 5 minutes how many of my updates arrived and their average price, so that
  I know the new prices are in before customers see them.

  Background:
    Given the simulator delivers one hour of price updates to the window function

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
