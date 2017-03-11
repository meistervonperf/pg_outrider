/* pg_outrider/pg_outrider--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_outrider" to load this file. \quit

CREATE FUNCTION pg_outrider_launch(regclass,
						   incrementMB int8 default 1,
						   watermarkMB int8 default 2)
RETURNS pg_catalog.int4 STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;
