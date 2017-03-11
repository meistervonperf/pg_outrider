# pg_outrider/Makefile

MODULES = pg_outrider

EXTENSION = pg_outrider
DATA = pg_outrider--1.0.sql
PGFILEDESC = "pg_outrider - background worker example"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
