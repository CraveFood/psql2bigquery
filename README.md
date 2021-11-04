# PostgreSQL to BigQuery

Install with: `pip install psql2bigquery`

Get usage instructions with: `psql2bigquery run --help`

## Sample usage

```
poetry run psql2bigquery run \
--db-host localhost \
--db-port 5432 \
--db-user username \
--db-password secret-password \
--db-name my_api \
--gcp-project my-project \
--gcp-dataset my_api \
--include table_name_a \
--include table_name_b \
--gcp-credential-path /path/to/credential.json
```

## Logging

There's a possibility to use Sentry.io for error logging.

Just set the environment variable `SENTRY_DSN` and psql2bigquery will automatically configure the logger.

Additionally, the environment variable `ENV` can be used as Sentry environment.


## Contributing

- Fork this project
- Install dependencies with `make dependencies`
  - Make sure you have Python 3 installed. (pyenv)[https://github.com/pyenv/pyenv#installation] is highly recommended
- You can test the client locally (without installing the package) with `poetry run psql2bigquery <command>`
- Make a PR with as much details as possible
