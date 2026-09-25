-- Runs once, as the bootstrap superuser, when the (tmpfs) cluster is created.
--
-- Roles mirror a managed deployment (RDS): the application role is not a
-- superuser. It owns the jetstream database and can create scratch databases
-- for storage tests. The reader role models reader pods, which only SELECT and
-- LISTEN (see specs/notes/2026-09-25-disaggregated-storage-v2-design.md §24).
-- Any write from a reader-role connection fails, the same as in production.

CREATE ROLE jetstream LOGIN CREATEDB PASSWORD 'jetstream';
CREATE ROLE jetstream_reader LOGIN PASSWORD 'jetstream_reader';

CREATE DATABASE jetstream OWNER jetstream;

\connect jetstream

GRANT CONNECT ON DATABASE jetstream TO jetstream_reader;
GRANT USAGE ON SCHEMA public TO jetstream_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE jetstream IN SCHEMA public
    GRANT SELECT ON TABLES TO jetstream_reader;

-- pg_stat_statements lives in the maintenance database so the jetstream
-- database stays empty until `jetstream storage init` creates the schema.
\connect postgres

CREATE EXTENSION pg_stat_statements;
