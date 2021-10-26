# PostgreSQL to BigQuery

Install with: `pip install psql2bigquery`

Get usage instructions with: `psql2bigquery run --help`


## Logging

There's a possibility to use Sentry.io for error logging.

Just set the environment variable `SENTRY_DSN` and psql2bigquery will automatically configure the logger.


## Contributing

- Fork this project
- Install dependencies with `make dependencies`
  - Make sure you have Python 3 installed. (pyenv)[https://github.com/pyenv/pyenv#installation] is highly recommended
- You can test the client locally (without installing the package) with `poetry run psql2bigquery <command>`
- Make a PR with as much details as possible
