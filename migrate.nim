import db_connector/db_sqlite
from std/strformat import fmt
from std/os import fileExists
from std/strutils import parseInt, contains, split, formatFloat, ffDecimal
from std/times import parse, Datetime, format, epochTime
from logging import info, warn, error


type Log = object
  date: string
  remoteIP: string
  httpMethod: string
  requestURI: string
  statusCode: string
  responseSize: int
  referrer: string
  userAgent: string
  nonStandard: string
  remoteUser: string
  authenticatedUser: string


proc convertDateFormat(nginxDate: string): string =
  let parsedDate: DateTime = parse(nginxDate, "d-MMM-yyyy:HH:mm:ss")
  return parsedDate.format("yyyy-MM-dd HH:mm:ss")

proc getDbConnection(dbPath: string): DbConn =
  info(fmt"Opening Database connection to {dbPath}")

  try:
    let connection: DbConn = open(dbPath, "", "", "")
    return connection
  except db_sqlite.DbError as e:
    error(fmt"Could not open or connect to database: {e.msg}")
    quit(1)

proc closeDbConnection(db: DbConn) =
  info("Closing Database connection")

  try:
    db.close()
  except db_sqlite.DbError as e:
    warn(fmt"Could not close database: {e.msg}")
    quit(1)

proc createTables(db: DbConn) =
  info("Creating database tables")

  db.exec(sql"BEGIN TRANSACTION")

  db.exec(sql"""CREATE TABLE IF NOT EXISTS date
          (
            id INTEGER PRIMARY KEY,
            date TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS remoteIP
          (
            id INTEGER PRIMARY KEY,
            remoteIP TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS httpMethod
          (
            id INTEGER PRIMARY KEY,
            httpMethod TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS requestURI
          (
            id INTEGER PRIMARY KEY,
            requestURI TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS statusCode
          (
            id INTEGER PRIMARY KEY,
            statusCode TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS responseSize
          (
            id INTEGER PRIMARY KEY,
            responseSize TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS referrer
          (
            id INTEGER PRIMARY KEY,
            referrer TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS userAgent
          (
            id INTEGER PRIMARY KEY,
            userAgent TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS nonStandard
          (
            id INTEGER PRIMARY KEY,
            nonStandard TEXT UNIQUE,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS remoteUser
          (
            id INTEGER PRIMARY KEY,
            remoteUser TEXT UNIQUE,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS authenticatedUser
          (
            id INTEGER PRIMARY KEY,
            authenticatedUser TEXT UNIQUE,
            count INTEGER NOT NULL DEFAULT 1
          )"""
  )

  db.exec(sql"""CREATE TABLE IF NOT EXISTS nginwho
          (
            id INTEGER PRIMARY KEY,
            date_id INTEGER NOT NULL,
            remoteIP_id INTEGER NOT NULL,
            httpMethod_id INTEGER NOT NULL,
            requestURI_id INTEGER NOT NULL,
            statusCode_id INTEGER NOT NULL,
            responseSize_id INTEGER NOT NULL,
            referrer_id INTEGER NOT NULL,
            userAgent_id INTEGER NOT NULL,
            nonStandard_id INTEGER,
            remoteUser_id INTEGER,
            authenticatedUser_id INTEGER,
            FOREIGN KEY (date_id) REFERENCES date(id),
            FOREIGN KEY (remoteIP_id) REFERENCES remoteIP(id),
            FOREIGN KEY (httpMethod_id) REFERENCES httpMethod(id),
            FOREIGN KEY (requestURI_id) REFERENCES requestURI(id),
            FOREIGN KEY (statusCode_id) REFERENCES statusCode(id),
            FOREIGN KEY (responseSize_id) REFERENCES responseSize(id),
            FOREIGN KEY (referrer_id) REFERENCES referrer(id),
            FOREIGN KEY (userAgent_id) REFERENCES userAgent(id),
            FOREIGN KEY (nonStandard_id) REFERENCES nonStandard(id),
            FOREIGN KEY (remoteUser_id) REFERENCES remoteUser(id),
            FOREIGN KEY (authenticatedUser_id) REFERENCES authenticatedUser(id)
          )"""
  )

  db.exec(sql"COMMIT")

  info("Created database tables")

proc insertOrUpdateColumn(db: DbConn, table, column, value: string): int64 =
  let row = db.getRow(sql(fmt"SELECT id, count FROM {table} WHERE {column} = ?"), value)

  if row[0] == "":
    let insertedRowId = db.insertID(sql"INSERT INTO ? (?, count) VALUES (?, 1)",
        table, column, value)

    return insertedRowId

  let updatedRowId = parseInt(row[0]).int64
  let newCount = parseInt(row[1]) + 1

  db.exec(sql"UPDATE ? SET count = ? WHERE id = ?", table, newCount, updatedRowId)

  return updatedRowId

proc insertLog(db: DbConn, logs: seq[Log]) =
  info(fmt"Writing {len(logs)} logs to database")

  db.exec(sql"BEGIN TRANSACTION")

  let insertQuery = """
    INSERT INTO nginwho 
     (
       date_id,
       remoteIP_id,
       httpMethod_id,
       requestURI_id,
       statusCode_id,
       responseSize_id,
       referrer_id,
       userAgent_id,
       nonStandard_id,
       remoteUser_id,
       authenticatedUser_id
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  """
  let preparedStmt = db.prepare(insertQuery)
  defer: preparedStmt.finalize()


  for log in logs:
    let
      dateId = insertOrUpdateColumn(db, "date", "date", log.date)
      remoteIPId = insertOrUpdateColumn(db, "remoteIP", "remoteIP", log.remoteIP)
      httpMethodId = insertOrUpdateColumn(db, "httpMethod", "httpMethod",
          log.httpMethod)
      requestURIId = insertOrUpdateColumn(db, "requestURI", "requestURI",
          log.requestURI)
      statusCodeId = insertOrUpdateColumn(db, "statusCode", "statusCode",
          log.statusCode)
      responseSizeId = insertOrUpdateColumn(db, "responseSize", "responseSize",
          $log.responseSize)
      referrerId = insertOrUpdateColumn(db, "referrer", "referrer", log.referrer)
      userAgentId = insertOrUpdateColumn(db, "userAgent", "userAgent", log.userAgent)
      nonStandardId = insertOrUpdateColumn(db, "nonStandard", "nonStandard",
          log.nonStandard)
      remoteUserId = insertOrUpdateColumn(db, "remoteUser", "remoteUser",
          log.remoteUser)
      authenticatedUserId = insertOrUpdateColumn(db, "authenticatedUser",
          "authenticatedUser", log.authenticatedUser)

    db.exec(
      preparedStmt,
      dateId,
      remoteIPId,
      httpMethodId,
      requestURIId,
      statusCodeId,
      responseSizeId,
      referrerId,
      userAgentId,
      nonStandardId,
      remoteUserId,
      authenticatedUserId
    )

  db.exec(sql"COMMIT")

proc migrateV1ToV2(v1DbName, v2DbName: string) =
  info(fmt"Migrating v1 database at '{v1DbName}' to v2 database at '{v2DbName}'")

  if not fileExists(v1DbName):
    error(fmt("V1 database does not exist at {v1DbName}"))
    quit(1)


  let v1Db = getDbConnection(v1DbName)
  let v2Db = getDbConnection(v2DbName)

  defer:
    closeDbConnection(v1Db)
    closeDbConnection(v2Db)

  var totalRecords: string

  try:
    totalRecords = v1db.getRow(sql"SELECT COUNT(*) from nginwho")[0]
    info(fmt"{v1DbName} contains {totalRecords} records")
  except DbError as e:
    error(fmt"Could not count rows in {v1DbName}: {e.msg}")
    let recoveryCommand = fmt"sqlite3 {v1DbName} '.recover' | sqlite3 {v1DbName.split('.')[0]}_recovered.db"
    info(fmt"Retry after recovering your DB with: {recoveryCommand}")
    quit(1)

  createTables(v2Db)

  let selectStatement = sql"""
    SELECT date, remoteIP, httpMethod, requestURI, statusCode, responseSize, 
           referrer, userAgent, remoteUser, authenticatedUser
    FROM nginwho
    WHERE date IS NOT NULL AND date != ''
    LIMIT ? OFFSET ?
  """

  const migrationBatchSize = 50_000

  var
    logs: seq[Log]
    batchCount = 0
    offset = 0
    totalRecordsToProcess = parseInt(totalRecords)

  while totalRecordsToProcess > 0:
    info(fmt"🔥 Processing records from offset {offset} in batches of {migrationBatchSize}")

    var rows: seq[Row]

    try:
      rows = v1Db.getAllRows(selectStatement, migrationBatchSize, offset)
      if len(rows) == 0:
        break
    except DbError as e:
      error(fmt"Could not get rows with limit of {migrationBatchSize} from offset {offset}: {e.msg}")
      break

    for row in rows:
      let httpMethod = row[2]
      if httpMethod.contains("\\") or httpMethod.contains("{"):
        warn(fmt"Skipping row due to bad format: {row}")
        continue

      logs.add(
        Log(
          date: convertDateFormat(row[0]),
          remoteIP: row[1],
          httpMethod: httpMethod,
          requestURI: row[3],
          statusCode: row[4],
          responseSize: parseInt(row[5]),
          referrer: row[6],
          userAgent: row[7],
          remoteUser: row[8],
          authenticatedUser: row[9]
        )
      )

      batchCount += 1
      if batchCount >= migrationBatchSize:
        let start = epochTime()
        insertLog(v2Db, logs)
        let elapsed = epochTime() - start
        let elapsedStr = elapsed.formatFloat(format = ffDecimal, precision = 3)
        info(fmt"Row insertion took {elapsedStr} seconds")

        batchCount = 0
        logs = @[]

    offset += migrationBatchSize
    totalRecordsToProcess = abs(totalRecordsToProcess - offset)

  # if there are leftovers, add them
  if batchCount > 0:
    info(fmt"Adding {batchCount} leftovers")
    insertLog(v2Db, logs)

  info(fmt"Processed {totalRecords} records")

when isMainModule:
  migrateV1ToV2("nginwho_v1.db", "nginwho_v2.db")
