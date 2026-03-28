import mysql.connector


def make_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="actowiz",
        database="world_portal"
    )


def create_country_table():
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS country_urls (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_name VARCHAR(255),
        country_url VARCHAR(500) UNIQUE,
        status VARCHAR(50) DEFAULT 'Pending'
    )
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def create_region_table():
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS region_urls (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_name VARCHAR(255),
        country_url VARCHAR(500),
        parent_region VARCHAR(255),
        region_name VARCHAR(255),
        region_url VARCHAR(500) UNIQUE,
        level_no INT,
        status VARCHAR(50) DEFAULT 'Pending'
    )
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def insert_country_urls(rows):
    if not rows:
        return

    conn = make_connection()
    cursor = conn.cursor()

    query = """
    INSERT IGNORE INTO country_urls (
        country_name,
        country_url,
        status
    )
    VALUES (%s, %s, %s)
    """

    data = [
        (
            row.get("Country_name"),
            row.get("Country_URL"),
            row.get("Status", "Pending")
        )
        for row in rows
    ]

    cursor.executemany(query, data)
    conn.commit()

    print(f"Inserted Country Rows: {cursor.rowcount}")

    cursor.close()
    conn.close()


def insert_region_urls(rows):
    if not rows:
        return

    conn = make_connection()
    cursor = conn.cursor()

    query = """
    INSERT IGNORE INTO region_urls (
        country_name,
        country_url,
        parent_region,
        region_name,
        region_url,
        level_no,
        status
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    data = [
        (
            row.get("country_name"),
            row.get("country_url"),
            row.get("parent_region"),
            row.get("region_name"),
            row.get("region_url"),
            row.get("level_no"),
            row.get("status", "Pending")
        )
        for row in rows
    ]

    cursor.executemany(query, data)
    conn.commit()

    print(f"Inserted Region Rows: {cursor.rowcount}")

    cursor.close()
    conn.close()


def fetch_pending_country_urls():
    conn = make_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
    SELECT country_name, country_url
    FROM country_urls
    WHERE status = 'Pending'
      AND country_url IS NOT NULL
      AND country_url != ''
    """

    cursor.execute(query)
    data = cursor.fetchall()

    cursor.close()
    conn.close()
    return data


def update_country_status(country_url, status="Done"):
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    UPDATE country_urls
    SET status = %s
    WHERE country_url = %s
    """

    cursor.execute(query, (status, country_url))
    conn.commit()

    cursor.close()
    conn.close()

def create_postal_table():
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS postal_codes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        country_region TEXT,
        postal_code VARCHAR(100),
        UNIQUE KEY unique_postal (country_region(255), postal_code)
    )
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def update_region_status(region_url, status="Done"):
    conn = make_connection()
    cursor = conn.cursor()

    query = """
    UPDATE region_urls
    SET status = %s
    WHERE region_url = %s
    """

    cursor.execute(query, (status, region_url))
    conn.commit()

    cursor.close()
    conn.close()

def insert_postal_codes(rows):
    if not rows:
        return

    conn = make_connection()
    cursor = conn.cursor()

    query = """
    INSERT IGNORE INTO postal_codes (
        country_region,
        postal_code
    )
    VALUES (%s, %s)
    """

    data = [
        (
            row.get("country_region"),
            row.get("postal_code")
        )
        for row in rows
    ]

    cursor.executemany(query, data)
    conn.commit()

    print(f"Inserted Postal Rows: {cursor.rowcount}")

    cursor.close()
    conn.close()