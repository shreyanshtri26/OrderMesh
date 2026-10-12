-- Local development only: one Postgres server, one database per service.
-- Services never share a database; in production each gets its own Neon project.
CREATE DATABASE orders;
CREATE DATABASE inventory;
CREATE DATABASE payments;
CREATE DATABASE notifications;
