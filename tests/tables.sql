CREATE TABLE entities (
    added_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id BINARY(16) NOT NULL,
    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    body MEDIUMBLOB,
    UNIQUE KEY (id),
    KEY (updated)
) ENGINE=InnoDB;

CREATE TABLE index_user_id (
    entity_id BINARY(16) NOT NULL UNIQUE,
    user_id CHAR(32) NOT NULL,
    PRIMARY KEY (user_id, entity_id)
) ENGINE=InnoDB;

CREATE TABLE index_user_name (
    entity_id BINARY(16) NOT NULL UNIQUE,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (first_name, last_name, entity_id)
) ENGINE=InnoDB;

CREATE TABLE index_foo (
    entity_id BINARY(16) NOT NULL UNIQUE,
    bar INTEGER NOT NULL,
    PRIMARY KEY (bar, entity_id)
) ENGINE=InnoDB;
