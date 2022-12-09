Feature: citus
  We should check that coordinator discovers and registers workers and clients don't have errors when worker cluster switches over

  Scenario: check that worker cluster is registered in the coordinator
    Given I start postgres0 in citus group 0
    And I start postgres1 in citus group 1
    Then postgres0 is a leader in a group 0 after 10 seconds
    And postgres1 is a leader in a group 1 after 10 seconds
    When I start postgres2 in citus group 1
    Then replication works from postgres1 to postgres2 after 15 seconds
    And postgres1 is registered in the coordinator postgres0 as the worker in group 1

  Scenario: worker switchover doesn't break client queries on the coordinator
    Given I create a distributed table on postgres0
    And I start a thread inserting data on postgres0
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And replication works from postgres2 to postgres1 after 15 seconds
    And postgres2 is registered in the coordinator postgres0 as the worker in group 1
    And a thread is still alive
    When I run patronictl.py switchover batman --group 1 --force
    Then I receive a response returncode 0
    And replication works from postgres1 to postgres2 after 15 seconds
    And postgres1 is registered in the coordinator postgres0 as the worker in group 1
    And a thread is still alive
    When I stop a thread
    Then a distributed table on postgres0 has expected rows

  Scenario: worker primary restart doesn't break client queries on the coordinator
    Given I cleanup a distributed table on postgres0
    And I start a thread inserting data on postgres0
    When I run patronictl.py restart batman postgres1 --group 1 --force
    Then I receive a response returncode 0
    And replication works from postgres1 to postgres2 after 15 seconds
    And postgres1 is registered in the coordinator postgres0 as the worker in group 1
    And a thread is still alive
    When I stop a thread
    Then a distributed table on postgres0 has expected rows
