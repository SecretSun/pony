# Pony Introduction

Consumption of MySQL Row mode binglog, parsing to the downstream node, for incremental operations.
Pure Python Implementation of MySQL replication protocol build on top of PyMYSQL. This allow you to receive event like insert, update, delete with their datas and raw SQL queries.

## Use cases
* MySQL to MySQL database replication
* MySQL to DB Proxy (MyCAT Cobar Kingshard...)

## Documentation
Program is divided into two modules, simulation mysql consumption binglog implementation method, io worker pull binlog transfer to sql worker to convert.

## Configuration Examples
* BaseLog: basis log config
* BinLogDBServer: Binlog Server (MySQL Master)
* DbProxy: Accept Server
* LogDBServer: Conversion log to keep machine (Temporarily achieved to keep to mysql)
* Rule: Corresponding scheme, Corresponding table
* BinLog: Start binlog pos
* SqlWorker: Sql Work Server address

## Project status
* Python >= 2.6
* MySQL 5.5 and 5.6

## Projects using this library
* Parse binlog lib https://github.com/noplay/python-mysql-replication

## Future cases
* MySQL to Redis
* MySQL to Kafka
* Keep log to Kafka

