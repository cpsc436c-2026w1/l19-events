@JS01 @live
Feature: Price updates of my store, on AWS
  The failing batch on a real stream, a real Lambda function and a real queue. This scenario calls AWS and waits
  for a 5-minute window to end: run `uv run python -m aws.deploy up` first, then `uv run behave --tags=live`.

  @R080
  Scenario: One bad record does not stop the other records of its batch, on a real stream
    Given the l19 resources are up in my AWS account
    When 20 good updates and one without a price are written to the price table
    And the 5-minute window has ended
    Then 20 updates are counted in the window table, each once
    And the on-failure queue holds 1 record for a batch of 1
