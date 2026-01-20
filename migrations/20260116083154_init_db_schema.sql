-- Initial DB schema for Quixote v0.1.0

CREATE TABLE IF NOT EXISTS quixote_info (
    version VARCHAR NOT NULL,
    PRIMARY KEY (version)
);

CREATE TABLE IF NOT EXISTS event_descriptor(
    chain_id NUMERIC(20,0) NOT NULL,
    event_hash BYTEA NOT NULL,
    event_signature BYTEA NOT NULL,
    event_name VARCHAR(40) NOT NULL,
    first_block NUMERIC(20,0),
    last_block NUMERIC(20,0),
    PRIMARY KEY (chain_id, event_hash)
);

INSERT INTO quixote_info (version) VALUES ('0.1.0');
