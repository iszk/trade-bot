
default:
    @just --choose

psql:
    @docker compose exec -it db psql -U bot_user -d trading_db

psql-postgres:
    @docker compose exec -it db psql -U postgres
