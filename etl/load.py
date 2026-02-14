def upsert(cursor, table, columns, rows):
    col_str = ",".join(columns)
    placeholders = ",".join(["%s"] * len(columns))

    updates = ",".join([
        "{0}=VALUES({0})".format(c)
        for c in columns if c != "pid"
    ])

    sql = """
        INSERT INTO {0} ({1})
        VALUES ({2})
        ON DUPLICATE KEY UPDATE {3}
    """.format(table, col_str, placeholders, updates)

    cursor.executemany(sql, rows)
# Loding the data to the target