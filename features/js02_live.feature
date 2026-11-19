@JS02 @live
Feature: The night's export files, on AWS
  The agent for a held file as a Lambda function that S3 starts. This scenario calls AWS and the course LLM
  gateway (at most 24 model calls): run `uv run python -m aws.deploy agent-up` first, then
  `uv run behave --tags=live`, then `uv run python -m aws.deploy agent-down`.

  @R150
  Scenario: A new file in the raw prefix starts the agent, and code checks its cause
    Given the l19 agent resources are up in my AWS account
    When the two files with injected faults are uploaded to the raw prefix
    Then a result is written for each file within 11 minutes
    And every named cause is either confirmed by code or the fix is "escalate"
    And the agent made at most 24 model calls in all
