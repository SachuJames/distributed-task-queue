---
name: Bug report
about: Something is not working as documented
title: "[bug] "
labels: bug
---

## What happened
A clear description of the unexpected behavior.

## Expected behavior
What the contract or docs say should happen (cite the section if you can).

## Reproduction
Steps, commands, or a minimal script. Include:
- service (api / worker / dashboard / cli)
- queue, task type, and task id if relevant
- Redis state if relevant (`dtq` CLI output helps)

## Environment
- DTQ version / commit:
- Python and Redis versions:
- How it was run (local, docker compose, other):

## Logs
Relevant log lines (JSON log fields welcome). Remove any secrets before pasting.
