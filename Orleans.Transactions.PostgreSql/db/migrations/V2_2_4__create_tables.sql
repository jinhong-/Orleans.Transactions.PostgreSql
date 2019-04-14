CREATE TABLE transaction_metadata
(
  state_id              STRING NOT NULL,
  committed_sequence_id INT    NOT NULL,
  etag                  STRING NOT NULL,
  value                 JSONB  NOT NULL,
  PRIMARY KEY (state_id)
);

CREATE TABLE transaction_state
(
  state_id            STRING    NOT NULL,
  sequence_id         INT       NOT NULL,
  transaction_manager JSONB     NOT NULL,
  value               JSONB     NOT NULL,
  timestamp           TIMESTAMP NOT NULL,
  transaction_id      STRING    NOT NULL,
  PRIMARY KEY (state_id, sequence_id)
);