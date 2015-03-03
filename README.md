## brobbot-pg-brain

A Postgres-backed brain for [brobbot](https://npmjs.org/package/brobbot).

Requires Postgres 9.4 or higher.

## Usage

In your [brobbot-instance](https://github.com/b3nj4m/brobbot-instance):

```bash
npm install --save brobbot-pg-brain
./index.sh -b pg
```

## Configuration

### URL

Use one of the following environment variables to set the Postgres URL:

- `HEROKU_POSTGRESQL_*_URL`
- `DATABASE_URL`
- `POSTGRESQL_URL`

```bash
HEROKU_POSTGRESQL_BLUE_URL=postgres://user:password@localhost/brobbot ./index.sh -b pg
```

### Table name

Set `BROBBOT_PG_TABLE_NAME` to change the default table name (`'brobbot'`).

```bash
BROBBOT_PG_TABLE_NAME=robot ./index.sh -b pg
```

### Data key prefix

Set `BROBBOT_PG_DATA_PREFIX` to change the default key prefix (`'data:'`).

```bash
BROBBOT_PG_DATA_PREFIX=brobbot-data: ./index.sh -b pg
```
