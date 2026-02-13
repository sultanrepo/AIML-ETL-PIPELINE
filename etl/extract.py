def extract_rows(cursor, table, last_ts):
    sql = """
        SELECT *
        FROM {0}
        WHERE updated_on > %s
        ORDER BY updated_on
    """.format(table)

    cursor.execute(sql, (last_ts,))
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return rows, columns
# Extraction of data from the source database based on the last updated timestamp.