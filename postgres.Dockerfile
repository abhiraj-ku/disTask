FROM postgres:17-alpine3.21

WORKDIR /app


COPY pkg/db/db.sql /docker-entrypoint-initdb.d/