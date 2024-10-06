# sqlite3 Migration Speed - Nim vs Python

Temp repo to seek advice on the speed difference between almost identical Nim and Python code to migrate sqlite3 data of [nginwho](https://github.com/pouriyajamshidi/nginwho) from v1 to v2 schema.

Both the Nim and Python versions are run locally and operate on the same database file. The result is Python _always_ finishes **200~250** seconds _faster_ than Nim.

The tests are run on a database with **4,100,000** records but the actual production database contains **110,000,000** records and counting.

## Program Versions

Nim version:

```console
> nim --version
Nim Compiler Version 2.2.0 [Linux: amd64]
Compiled at 2024-10-02
Copyright (c) 2006-2024 by Andreas Rumpf

git hash: 78983f1876726a49c69d65629ab433ea1310ece1
active boot switches: -d:release

```

Python version:

```console
> python3 --version
Python 3.12.7
```

## Speed Difference

### Nim 2.2.0 - ORC

`nim c -d:release --opt:speed --mm:orc --threads:off migrate.nim` -- Executed in **626.82** secs

`nim c -d:release --opt:speed --mm:orc --threads:on migrate.nim` -- Executed in **700.40** secs

### Nim 2.2.0 - ARC

`nim c -d:release --opt:speed --mm:arc --threads:off migrate.nim` -- Executed in **667.15** secs

`nim c -d:release --opt:speed --mm:arc --threads:on migrate.nim` -- Executed in **681.39** secs

### Python 3.12.7

`python3.12.7 migrate.py` -- Executed in **428.27** secs

## Nim profiler

I also profiled the Nim code and here is the result:

```console
total executions of each stack trace:
Entry: 1/170 Calls: 99911/332094 = 30.09% [sum: 99911; 99911/332094 = 30.09%]
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/private/dbutils.nim: dbFormat 236890/332094 = 71.33%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: setupQuery 130750/332094 = 39.37%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: getRow 151432/332094 = 45.60%
  /home/tmp/migrate.nim: insertOrUpdateColumn 300983/332094 = 90.63%
  /home/tmp/migrate.nim: insertLog 302351/332094 = 91.04%
  /home/tmp/migrate.nim: migrateV1ToV2 332093/332094 = 100.00%
Entry: 2/170 Calls: 81590/332094 = 24.57% [sum: 181501; 181501/332094 = 54.65%]
  /home/.choosenim/toolchains/nim-2.2.0/lib/system/iterators.nim: dbFormat 236890/332094 = 71.33%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: tryExec 110352/332094 = 33.23%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: exec 110735/332094 = 33.34%
  /home/tmp/migrate.nim: insertOrUpdateColumn 300983/332094 = 90.63%
  /home/tmp/migrate.nim: insertLog 302351/332094 = 91.04%
  /home/tmp/migrate.nim: migrateV1ToV2 332093/332094 = 100.00%
Entry: 3/170 Calls: 24737/332094 = 7.45% [sum: 206238; 206238/332094 = 62.10%]
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: dbQuote 48440/332094 = 14.59%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/private/dbutils.nim: dbFormat 236890/332094 = 71.33%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: setupQuery 130750/332094 = 39.37%
  /home/.nimble/pkgs2/db_connector-0.1.0-d68319e3785fa937f0465ea915e942b61b6b5442/db_connector/db_sqlite.nim: getRow 151432/332094 = 45.60%
  /home/tmp/migrate.nim: insertOrUpdateColumn 300983/332094 = 90.63%
  /home/tmp/migrate.nim: insertLog 302351/332094 = 91.04%
  /home/tmp/migrate.nim: migrateV1ToV2 332093/332094 = 100.00%
```
