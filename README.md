# JustASimpleToyDB

This is a practice project for designing and creating custom DB from scratch

## How to run

Run the application with
```
go run cmd/justasimpletoydb/main.go
```
This starts REPL session with the running application which will interpret and run your SQL
```
CREATE TABLE animals (id INT, name TEXT);
INSERT INTO animals VALUES (1, 'FROG');
INSERT INTO animals VALUES (2, 'SNAKE');
SELECT * FROM animals;
```


## Design

- Postgres-like
- 16kB pages
- multi-file storage
- one database per application stored at `data/` with `catalog.json` deciding the schema of it and individual `.tbl` files storing the data of each table
