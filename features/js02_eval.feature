@JS02 @eval
Feature: The agent's causes, checked by an eval
  The agent's answer can differ between runs, so a scenario with one fixed answer does not test it. An eval runs
  the agent on every injected fault and scores the named cause in code; the gate fails below a threshold.
  The eval runs on the stub model. Set L19_LIVE=1 to run it on the course LLM gateway (at most 16 model calls).

  @R150
  Scenario: The eval reaches the threshold
    Given the eval of the agent on its injected faults
    When the eval runs
    Then the gate passes at the threshold
    And every injected fault has a score in the log
