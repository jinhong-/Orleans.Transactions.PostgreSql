CREATE TABLE transaction_metadata
(
  state_id              VARCHAR(255) NOT NULL,
  committed_sequence_id INT    NOT NULL,
  etag                  VARCHAR(255) NOT NULL,
  value                 TEXT  NOT NULL,
  PRIMARY KEY (state_id)
);

CREATE TABLE transaction_state
(
  state_id            VARCHAR(255)    NOT NULL,
  sequence_id         INT       NOT NULL,
  transaction_manager TEXT     NOT NULL,
  value               TEXT     NOT NULL,
  timestamp           TIMESTAMP NOT NULL,
  transaction_id      VARCHAR(255)    NOT NULL,
  PRIMARY KEY (state_id, sequence_id)
);