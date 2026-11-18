@JS04
Feature: The latest price while prices change
  When I look up a product's price in the app while the stores are changing prices (50 shoppers at once), I want
  the answer in well under a second and never a price that was replaced more than 5 minutes ago, so that the
  price I see is the one I pay.

  @R010
  Scenario: 50 shoppers get their answers fast
    Given the simulator streams the price updates from 09:20 for 4 minutes
    When 50 simulated shoppers ask for latest prices during that time
    Then the p99 latency of the queries is under 50 ms

  @R020
  Scenario: No answer is older than the window when the handler keeps up
    Given the simulator streams the price updates from 09:20 for 4 minutes
    When 50 simulated shoppers ask for latest prices during that time
    Then the largest staleness is at most 300 seconds
    And the report says the answers are within the window

  @R030
  Scenario: A handler that falls behind is reported
    Given the simulator streams the price updates from 09:20 for 10 minutes
    And the handler needs 4 seconds for each update
    When 50 simulated shoppers ask for latest prices during that time
    Then the largest staleness is more than 300 seconds
    And the report says the handler is behind

  @R040
  Scenario: Of two updates for one product, the store keeps the newer one (simulated)
    Given an in-memory price store
    When an update observed at 09:20:05 and then an update observed at 09:20:01 are written for one product
    Then the first write wins and the second is refused
    And the stored price is the one observed at 09:20:05
