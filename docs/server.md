# Server Usage

## Development Server

Run the development server with auto-reload:

```bash
just dev-server
```

The server listens on `127.0.0.1:27017`.

NOTE: This requires cargo-watch as a dependency to run

## MongoDB Shell (mongosh)

Connect to the running server using mongosh:

```bash
mongosh mongodb://localhost:27017
```

Use MongoDB shell commands to interact with the database. Note: This is a learning-oriented implementation with limited features.
