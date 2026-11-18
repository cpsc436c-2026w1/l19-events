@JS01
Feature: Constraints on the price updates
  The constraints (RF) of the feedback, each a property of the running system that a scenario passes or fails.
  The planted case of each scenario comes from the simulator's files in events/price-updates/.

  Background:
    Given the simulator delivers one hour of price updates to the window function

  @RF010
  Scenario: A price update delivered twice is counted once
    Given the simulator delivers 4 of the updates a second time
    Then window 09:25 has the simulator's truth count for every store
