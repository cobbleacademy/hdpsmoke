-- TwoSchemaPostgresContainerTest: a production-shaped database. The app connects as
-- hsm_app, a plain login with the Postgres-default search_path ("$user", public), so
-- the test proves the app sets its own search_path rather than relying on the role.
-- hsm_app owns both schemas, as Flyway needs to create tables in them.
CREATE ROLE hsm_app LOGIN PASSWORD 'hsm_app_pw';
CREATE SCHEMA hsm_crypto AUTHORIZATION hsm_app;
CREATE SCHEMA hsm_access AUTHORIZATION hsm_app;
