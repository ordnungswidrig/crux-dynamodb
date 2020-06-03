# Crux TX-Log on AWS dynamodb

## Caveat

This is untested and experimental. Do not use anywhere near production.

## Design

Write a crux tx-log into a dynamodb table. The sort key contains the tx-id.
The partition (hash) key value can be configured and thus supports multiple
crux tx logs on the same dynamodb table.

## Instructions

See `user.clj` for some tests.
