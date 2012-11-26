-- [CHANGELOG]
-- 2012/11/15 - 1. Write numeric error is not because encoding problem, write in text has
--                 no issue, no need to create database with 'SQL_ASCII' encoding.
--              2. As above description, do not use BIGINT_UNSIGNED and change all 
--                 BIGINT_UNSIGNED fields to text.
-- 2012/11/14 - 1. Writing numeric value to database in C returns below error,
--                 "ERROR: bind message supplies 1 parameters, but prepared statement "" requires 0"
--                 Seems the database encoding problem, here is the way to change database
--                 encoding.
--                 1. Change template1 encoding to 'SQL_ASCII'.
--                    [http://journal.tianhao.info/2010/12/postgresql-change-default-encoding-of-new-databases-to-utf-8-optional/]
--                 2. Create database,
--                    "CREATE DATABASE testdb WITH TEMPLATE = template1 OWNER = test ENCODING = 'SQL_ASCII';"
--                 3. "\l" or "SELECT * FROM pg_database" to check testdb encoding type.


CREATE SCHEMA dispatcher;
SET search_path TO dispatcher,public;
-- uint64_t (http://bytes.com/topic/postgresql/answers/423780-numeric-type-problems)
--CREATE DOMAIN BIGINT_UNSIGNED NUMERIC(20,0) CHECK (value >= 0 and value < '18446744073709551616'::numeric(20,0));

CREATE TABLE services (
	name			text			PRIMARY KEY
);

CREATE TABLE workers (
	name			text			PRIMARY KEY,
	service_name		text			REFERENCES services ON DELETE SET NULL,
	address			text			NOT NULL,

	-- heartbeat
--	heartbeat_deadtime	BIGINT_UNSIGNED,
--	heartbeat_keepalive	BIGINT_UNSIGNED,
	heartbeat_deadtime	text			NOT NULL,
	heartbeat_keepalive	text			NOT NULL,

	--timeout
--	timeout_old		BIGINT_UNSIGNED,
--	timeout_new		BIGINT_UNSIGNED,
--	timeout_interval	BIGINT_UNSIGNED,
--	retries			int
	timeout_old		text			NOT NULL,
	timeout_new		text			NOT NULL,
	timeout_interval	text			NOT NULL,
	retries			text			NOT NULL
);

CREATE TABLE tasks (
	taskID			text			PRIMARY KEY,
--	status			BIGINT_UNSIGNED,
--	dispatched		int,
	status			text			NOT NULL,
	dispatched		text			NOT NULL,

--	create_time		BIGINT_UNSIGNED,
	create_time		text			NOT NULL,
	-- timeout
--	timeout_old		BIGINT_UNSIGNED,
--	timeout_new		BIGINT_UNSIGNED,
--	timeout_interval	BIGINT_UNSIGNED,
	timeout_old		text			NOT NULL,
	timeout_new		text			NOT NULL,
	timeout_interval	text			NOT NULL,
	
	service_name		text			REFERENCES services ON DELETE SET NULL,
	method			text			NOT NULL,
	data			text,
	client_str		text			NOT NULL,
	worker_str		text
);
