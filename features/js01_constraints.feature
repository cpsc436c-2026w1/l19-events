@JS01
Feature: Constraints on the price updates
  The constraints (RF) of the feedback, each a property of the running system that a scenario passes or fails.
  The planted case of each scenario comes from the simulator's files in events/price-updates/.

  Background:
    Given the simulator delivers one hour of price updates to the window function

  @RF010
  Scenario: A price update delivered twice is counted once
    Given the simulator delivers 4 of the updates a second time
    Then the updates counted for every store between 09:25 and 09:30 equal the simulator's truth

  @RF020
  Scenario: A store that sends no update is shown as quiet, not with its last price
    Given the simulator does not send an update from 09:40 to 09:50
    Then between 09:40 and 09:50 every store is shown with no update and no price

  @RF030
  Scenario: A late update counts in the window of its event time
    Given the simulator holds the updates of store centre from 09:10 to 09:16
    Then the updates counted for every store between 09:10 and 09:15 equal the simulator's truth
