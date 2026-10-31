-- Analytics DB Initialization
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Pre-create roles so api_ro and worker_rw can connect cleanly
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'api_ro') THEN
        CREATE ROLE api_ro WITH LOGIN PASSWORD 'api_ro_secret' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'worker_rw') THEN
        CREATE ROLE worker_rw WITH LOGIN PASSWORD 'worker_rw_secret' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;
END
$$;

-- Grant connect on analytics_db
GRANT CONNECT ON DATABASE analytics_db TO api_ro;
GRANT CONNECT ON DATABASE analytics_db TO worker_rw;
