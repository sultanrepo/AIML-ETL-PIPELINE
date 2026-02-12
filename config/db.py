# -*- coding: utf-8 -*-
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def get_src_conn():
    return mysql.connector.connect(
        host=os.getenv("SRC_HOST"),
        database=os.getenv("SRC_DB"),
        user=os.getenv("SRC_USER"),
        password=os.getenv("SRC_PASS")
    )

def get_tgt_conn():
    return mysql.connector.connect(
        host=os.getenv("TGT_HOST"),
        database=os.getenv("TGT_DB"),
        user=os.getenv("TGT_USER"),
        password=os.getenv("TGT_PASS")
    )
