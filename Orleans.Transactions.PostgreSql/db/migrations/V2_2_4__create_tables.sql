CREATE TABLE transaction_metadata
(
  state_id              STRING,
  committed_sequence_id INT,
  etag                  STRING,
  value              JSONB,
  PRIMARY KEY (state_id)
);

CREATE TABLE transaction_state
(
  state_id            STRING,
  sequence_id         INT,
  transaction_manager JSONB,
  value               JSONB,
  timestamp           TIMESTAMP,
  transaction_id      STRING,
  PRIMARY KEY (state_id, sequence_id)
);