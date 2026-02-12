import json
import logging
from config.db import get_src_conn, get_tgt_conn
from etl.extract import extract_rows
from etl.transform import transform
from etl.load import upsert

TABLES = [
    "aiml_hod_aoa",
    "aiml_hod_bsc",
    "aiml_hod_bsip",
    "aiml_hod_pga"
]

logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

with open("state/sync_state.json") as f:
    state = json.load(f)

src = get_src_conn()
tgt = get_tgt_conn()

src_cur = src.cursor()
tgt_cur = tgt.cursor()

for table in TABLES:
    last_ts = state.get(table)
    rows, cols = extract_rows(src_cur, table, last_ts)

    if not rows:
        logging.info("No new data for %s", table)
        continue

    clean_rows = transform(rows)
    upsert(tgt_cur, table, cols, clean_rows)

    max_ts = max([row[cols.index("updated_on")] for row in rows])
    state[table] = max_ts.strftime("%Y-%m-%d %H:%M:%S")

    logging.info("Synced %s rows for %s", len(rows), table)

tgt.commit()

with open("state/sync_state.json", "w") as f:
    json.dump(state, f)

src_cur.close()
tgt_cur.close()
src.close()
tgt.close()
