@JS04 @live
Feature: The latest price while prices change, on AWS
  The same story on a real DynamoDB table. These scenarios call AWS: run `uv run python -m aws.deploy up` first,
  then `uv run behave --tags=live`, then `uv run python -m aws.deploy down`.

  @R040
  Scenario: Two writers at once on a real table
    Given the l19 resources are up in my AWS account
    When two clients race 50 rounds of conditional writes on one item
    Then every round has exactly one winner
    And the final item holds the newest observed time

  @R010 @R020
  Scenario: 50 clients read a real table while updates are written
    Given the l19 resources are up in my AWS account
    When 50 clients read latest prices for 20 seconds while updates are written
    Then the p99 latency of the reads is under 1000 ms
    And the largest staleness is under 5 seconds
