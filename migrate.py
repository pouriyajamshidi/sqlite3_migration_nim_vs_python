import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Log:
    date: str
    remote_ip: str
    http_method: str
    request_uri: str
    status_code: str
    response_size: str
    referrer: str
    user_agent: str
    remote_user: str
    authenticated_user: str
    non_standard: str = None


def convert_date_format(nginx_date: str) -> str:
    parsed_date = datetime.strptime(nginx_date, "%d-%b-%Y:%H:%M:%S")
    return parsed_date.strftime("%Y-%m-%d %H:%M:%S")


def create_tables(db: sqlite3.Cursor) -> None:
    print("Creating database tables")

    db.execute("BEGIN TRANSACTION")

    db.execute("""CREATE TABLE IF NOT EXISTS date
          (
            id INTEGER PRIMARY KEY,
            date TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS remoteIP
          (
            id INTEGER PRIMARY KEY,
            remoteIP TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS httpMethod
          (
            id INTEGER PRIMARY KEY,
            httpMethod TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS requestURI
          (
            id INTEGER PRIMARY KEY,
            requestURI TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS statusCode
          (
            id INTEGER PRIMARY KEY,
            statusCode TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS responseSize
          (
            id INTEGER PRIMARY KEY,
            responseSize TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS referrer
          (
            id INTEGER PRIMARY KEY,
            referrer TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS userAgent
          (
            id INTEGER PRIMARY KEY,
            userAgent TEXT UNIQUE NOT NULL,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS nonStandard
          (
            id INTEGER PRIMARY KEY,
            nonStandard TEXT UNIQUE,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS remoteUser
          (
            id INTEGER PRIMARY KEY,
            remoteUser TEXT UNIQUE,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS authenticatedUser
          (
            id INTEGER PRIMARY KEY,
            authenticatedUser TEXT UNIQUE,
            count INTEGER NOT NULL DEFAULT 1
          )""")

    db.execute("""CREATE TABLE IF NOT EXISTS nginwho
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
          )""")

    db.execute("COMMIT")

    print("Created database tables")


def insert_or_update_column(
    db: sqlite3.Cursor, table: str, column: str, value: str
) -> int:
    row = db.execute(
        f"SELECT id, count FROM {table} WHERE {column} = ?", (value,)
    ).fetchone()

    if row is None:
        db.execute(f"INSERT INTO {table} ({column}, count) VALUES (?, 1)", (value,))

        return 1

    updated_row_id = row[0]
    new_count = row[1] + 1

    db.execute(
        f"UPDATE {table} SET count = ? WHERE id = ?", (new_count, updated_row_id)
    )

    return updated_row_id


def insert_log(db: sqlite3.Cursor, logs: list[Log]) -> None:
    print(f"Writing {len(logs)} logs to database")

    db.execute("BEGIN TRANSACTION")

    insert_query = """
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

    for log in logs:
        date_id = insert_or_update_column(db, "date", "date", log.date)
        remote_ip_id = insert_or_update_column(
            db, "remoteIP", "remoteIP", log.remote_ip
        )
        http_method_id = insert_or_update_column(
            db, "httpMethod", "httpMethod", log.http_method
        )
        request_uri_id = insert_or_update_column(
            db, "requestURI", "requestURI", log.request_uri
        )
        status_code_id = insert_or_update_column(
            db, "statusCode", "statusCode", log.status_code
        )
        response_size_id = insert_or_update_column(
            db, "responseSize", "responseSize", str(log.response_size)
        )
        referrer_id = insert_or_update_column(db, "referrer", "referrer", log.referrer)
        user_agent_id = insert_or_update_column(
            db, "userAgent", "userAgent", log.user_agent
        )
        non_standard_id = insert_or_update_column(
            db, "nonStandard", "nonStandard", log.non_standard
        )
        remote_user_id = insert_or_update_column(
            db, "remoteUser", "remoteUser", log.remote_user
        )
        authenticated_user_id = insert_or_update_column(
            db, "authenticatedUser", "authenticatedUser", log.authenticated_user
        )

        db.execute(
            insert_query,
            (
                date_id,
                remote_ip_id,
                http_method_id,
                request_uri_id,
                status_code_id,
                response_size_id,
                referrer_id,
                user_agent_id,
                non_standard_id,
                remote_user_id,
                authenticated_user_id,
            ),
        )

    db.execute("COMMIT")


def migrate_v1_to_v2(v1_db_name: str, v2_db_name: str) -> None:
    print(f"Migrating v1 database at {v1_db_name} to v2 database at {v2_db_name}")

    if not os.path.exists(v1_db_name):
        print(f"V1 database does not exist at {v1_db_name}")
        exit()

    v1_db: sqlite3.Connection = sqlite3.connect(v1_db_name)
    v2_db: sqlite3.Connection = sqlite3.connect(v2_db_name)

    v1_db_cur: sqlite3.Cursor = sqlite3.connect(v1_db_name).cursor()
    v2_db_cur: sqlite3.Cursor = sqlite3.connect(v2_db_name).cursor()

    total_records: str

    try:
        total_records = v1_db_cur.execute("SELECT COUNT(*) from nginwho").fetchone()[0]
        print(f"{v1_db_name} contains {total_records} records")
    except Exception as e:
        print(f"Could not count rows in {v1_db_name}: {str(e)}")
        recovery_command = f"sqlite3 {v1_db_name} '.recover' | sqlite3 {v1_db_name.split('.')[0]}_recovered.db"
        print(f"Retry after recovering your DB with: {recovery_command}")
        exit(1)

    create_tables(v2_db_cur)

    select_statement = """
    SELECT date, remoteIP, httpMethod, requestURI, statusCode, responseSize, 
           referrer, userAgent, remoteUser, authenticatedUser
    FROM nginwho
    WHERE date IS NOT NULL AND date != ''
    LIMIT ? OFFSET ?
    """

    migration_batch_size = 50_000

    logs: list[Log] = []
    batch_count = 0
    offset = 0
    total_records_to_process = int(total_records)

    while total_records_to_process > 0:
        print(
            f"🔥 Processing records from offset {offset} in batches of {migration_batch_size}"
        )

        rows: list[tuple] = []

        try:
            rows = v1_db_cur.execute(
                select_statement, (migration_batch_size, offset)
            ).fetchall()
            if len(rows) == 0:
                break
        except Exception as e:
            print(
                f"Could not get rows with limit of {migration_batch_size} from offset {offset}: {str(e)}"
            )
            break

        for row in rows:
            http_method = row[2]
            if "\\" in http_method or "{" in http_method:
                print(f"Skipping row due to bad format: {row}")
                continue

            logs.append(
                Log(
                    date=convert_date_format(row[0]),
                    remote_ip=row[1],
                    http_method=http_method,
                    request_uri=row[3],
                    status_code=row[4],
                    response_size=row[5],
                    referrer=row[6],
                    user_agent=row[7],
                    remote_user=row[8],
                    authenticated_user=row[9],
                )
            )

            batch_count += 1
            if batch_count >= migration_batch_size:
                start = time.time()
                insert_log(v2_db, logs)
                elapsed = time.time() - start
                print(f"Row insertion took {elapsed:.3f} seconds")

                batch_count = 0
                logs = []

        offset += migration_batch_size
        total_records_to_process = abs(total_records_to_process - offset)

    # if there are leftovers, add them
    if batch_count > 0:
        print(f"Adding {batch_count} leftovers")
        insert_log(v2_db_cur, logs)

    print(f"Processed {total_records} records")

    v1_db.close()
    v2_db.close()


if __name__ == "__main__":
    migrate_v1_to_v2("nginwho_v1.db", "nginwho_v2.db")
