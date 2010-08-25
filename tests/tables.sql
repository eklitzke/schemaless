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
