-- MySQL Script generated by MySQL Workbench
-- Wed Aug 31 10:30:23 2022
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema test
-- -----------------------------------------------------


CREATE SCHEMA IF NOT EXISTS `test` ;
USE `test` ;


-- -----------------------------------------------------
-- Table `test`.`show`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`show` (
  `id` INT UNSIGNED NOT NULL,
  `title` VARCHAR(255) NOT NULL,
  `description` TEXT NULL,
  `release_year` YEAR NOT NULL,
  `duration` SMALLINT NOT NULL,
  `rating` ENUM('G','PG','PG-13','R','NC-17','NR','TV-14','TV-G','TV-MA','TV-PG','TV-Y','TV-Y7','TV-Y7-FV','UR') NULL,
  `date_added` DATETIME,
  `show_id` VARCHAR(45) NOT NULL,
  `type` VARCHAR(45) NULL,
  INDEX `idx_title` (`title` ASC),
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC))
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`category`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`category` (
  `category_id` TINYINT UNSIGNED NOT NULL,
  `category` VARCHAR(225) NOT NULL,
  PRIMARY KEY (`category_id`))
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`list_category`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`list_category` (
  `show_id` INT UNSIGNED NOT NULL,
  `category_id` TINYINT UNSIGNED NOT NULL,
  PRIMARY KEY (`show_id`, `category_id`),
  INDEX `fk_show_category_category_idx` (`category_id` ASC),
  INDEX `fk_show_category_show_idx` (`show_id` ASC),
  CONSTRAINT `fk_show_category_category`
    FOREIGN KEY (`category_id`)
    REFERENCES `test`.`category` (`category_id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE,
  CONSTRAINT `fk_show_category_show`
    FOREIGN KEY (`show_id`)
    REFERENCES `test`.`show` (`id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
)
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`country`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`country` (
  `country_id` TINYINT UNSIGNED NOT NULL,
  `country` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`country_id`))
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`show_country`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`show_country` (
  `country_id` TINYINT UNSIGNED NOT NULL,
  `show_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`country_id`, `show_id`),
  INDEX `fk_show_country_country_idx` (`country_id` ASC),
  INDEX `fk_show_country_show_idx` (`show_id` ASC),
  CONSTRAINT `fk_show_country_show`
    FOREIGN KEY (`show_id`)
    REFERENCES `test`.`show` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_show_country_country`
    FOREIGN KEY (`country_id`)
    REFERENCES `test`.`country` (`country_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`director`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`director` (
  `director_id` INT UNSIGNED NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `first_name` VARCHAR(45) NOT NULL,
  `last_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`director_id`),
  INDEX `idx_director_last_name` (`last_name` ASC))
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`show_director`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`show_director` (
  `director_id` INT UNSIGNED NOT NULL,
  `show_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`director_id`, `show_id`),
  INDEX `idx_fk_show_director_id` (`show_id` ASC),
  INDEX `fk_show_director_director_idx` (`director_id` ASC),
  CONSTRAINT `fk_show_director_director`
    FOREIGN KEY (`director_id`)
    REFERENCES `test`.`director` (`director_id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE,
  CONSTRAINT `fk_show_director_show`
    FOREIGN KEY (`show_id`)
    REFERENCES `test`.`show` (`id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE)
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`actor`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`actor` (
  `actor_id` INT UNSIGNED NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `first_name` VARCHAR(45) NOT NULL,
  `last_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`actor_id`),
  INDEX `idx_actor_last_name` (`last_name` ASC))
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `test`.`show_actor`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `test`.`show_actor` (
  `actor_id` INT UNSIGNED NOT NULL,
  `show_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`actor_id`, `show_id`),
  INDEX `idx_fk_show_actor_idx` (`show_id` ASC),
  INDEX `fk_show_actor_actor_idx` (`actor_id` ASC),
  CONSTRAINT `fk_show_actor_actor`
    FOREIGN KEY (`actor_id`)
    REFERENCES `test`.`actor` (`actor_id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE,
  CONSTRAINT `fk_show_actor_show`
    FOREIGN KEY (`show_id`)
    REFERENCES `test`.`show` (`id`)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
    )
DEFAULT CHARACTER SET = utf8;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;