CREATE TABLE IF NOT EXISTS `entities` (
  `added_id` int(11) NOT NULL AUTO_INCREMENT,
  `id` binary(16) NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `body` mediumblob,
  PRIMARY KEY (`added_id`),
  UNIQUE KEY `id` (`id`),
  KEY `updated` (`updated`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `index_birthdate` (
  `entity_id` binary(16) NOT NULL,
  `birthdate` varchar(64) NOT NULL DEFAULT '',
  PRIMARY KEY (`birthdate`,`entity_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `index_foo` (
  `entity_id` binary(16) NOT NULL,
  `bar` int(11) NOT NULL,
  PRIMARY KEY (`bar`,`entity_id`),
  UNIQUE KEY `entity_id` (`entity_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `index_user_id` (
  `entity_id` binary(16) NOT NULL,
  `user_id` char(32) NOT NULL,
  PRIMARY KEY (`user_id`,`entity_id`),
  UNIQUE KEY `entity_id` (`entity_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `index_user_name` (
  `entity_id` binary(16) NOT NULL,
  `first_name` varchar(255) NOT NULL,
  `last_name` varchar(255) NOT NULL,
  PRIMARY KEY (`first_name`,`last_name`,`entity_id`),
  UNIQUE KEY `entity_id` (`entity_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `index_todo_user_id` (
  `entity_id` binary(16) NOT NULL,
  `user_id` char(32) NOT NULL,
  PRIMARY KEY (`user_id`,`entity_id`),
  UNIQUE KEY `entity_id` (`entity_id`)
) ENGINE=InnoDB;
