# JustASimpleToyDB

This is a practice project for designing and creating custom DB from scratch

## How to run

Run the server with
```
go run cmd/server/main.go
```

Run the REPL session with
```
go run cmd/repl/main.go
```
With this you create a connection to the server where you can run commands like...

```
CREATE TABLE animals (id INT, name TEXT);
INSERT INTO animals VALUES (1, 'FROG');
INSERT INTO animals VALUES (2, 'SNAKE');
SELECT * FROM animals;
SELECT name FROM animals;
```


## Design

There are couple of directions I follow when designing this
- Postgres-like
- 16kB pages
- multi-file storage
- one database per application stored at `data/` with `catalog.json` deciding the schema of it and individual `.tbl` files storing the data of each table

Each statement (like `INSERT` or `SELECT`) has its own entry in `executor/` and `parser/`, former defining the database execution logic and latter defining the way we collect tokens for the execution and validity of the statement.
