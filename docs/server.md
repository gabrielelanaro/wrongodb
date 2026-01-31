# Server Usage

## Development Server

Run the development server with auto-reload:

```bash
just dev-server
```

The server listens on `127.0.0.1:27017`.

NOTE: This requires cargo-watch as a dependency to run

## Custom Address / Port

You can override the listen address in three ways (highest priority first):

1) CLI arg:

```bash
cargo run --bin server -- 127.0.0.1:27019
```

2) Environment variable `WRONGO_ADDR`:

```bash
WRONGO_ADDR=127.0.0.1:27019 cargo run --bin server
```

3) Environment variable `WRONGO_PORT` (binds on `127.0.0.1`):

```bash
WRONGO_PORT=27019 cargo run --bin server
```

## MongoDB Shell (mongosh)

Connect to the running server using mongosh:

```bash
mongosh mongodb://localhost:27017
```

Use MongoDB shell commands to interact with the database. Note: This is a learning-oriented implementation with limited features.
