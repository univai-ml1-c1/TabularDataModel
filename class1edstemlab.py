import pandas as pd
import sqlite3 as sq3
from pathlib import Path
from collections import OrderedDict
import os

PATHSTART = "."

dfcand = pd.read_csv(
    "/Users/conquer/Documents/iFusion/BYC/python/Environments/UNIV/TabularDataModel/data/candidates.txt", "|")
dfcand
dfcwci = pd.read_csv(
    "/Users/conquer/Documents/iFusion/BYC/python/Environments/UNIV/TabularDataModel/data/contributors_with_candidate_id.txt", "|")
dfcwci.head()


def init_db(db_file, schema):
    db = get_db(db_file)
    db.cursor().executescript(schema)
    db.commit()
    return db


def get_db(dbfile):
    sqlite_db = sq3.connect(Path(PATHSTART)/dbfile)
    return sqlite_db


myschema = """
DROP TABLE IF EXISTS "candidates";
DROP TABLE IF EXISTS "contributors";
CREATE TABLE "candidates" (
    "id" INTEGER PRIMARY KEY  NOT NULL ,
    "first_name" VARCHAR,
    "last_name" VARCHAR,
    "middle_name" VARCHAR,
    "party" VARCHAR NOT NULL
);
CREATE TABLE "contributors" (
    "id" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL,
    "last_name" VARCHAR,
    "first_name" VARCHAR,
    "middle_name" VARCHAR,
    "street_1" VARCHAR,
    "street_2" VARCHAR,
    "city" VARCHAR,
    "state" VARCHAR,
    "zip" VARCHAR, -- Notice that we are converting the zip from integer to string
    "amount" INTEGER,
    "date" DATETIME,
    "candidate_id" INTEGER NOT NULL,
    FOREIGN KEY(candidate_id) REFERENCES candidates(id)
);
"""
db = init_db("/tmp/concant.db", myschema)


def make_query(qrystr):
    c = db.cursor().execute(qrystr)
    return c.fetchall()


def make_frame(list_of_tuples, legend):
    framelist = []
    for i, cname in enumerate(legend):
        framelist.append((cname, [e[i] for e in list_of_tuples]))
    return pd.DataFrame.from_dict(OrderedDict(framelist))


dfcand.to_sql("candidates", db, if_exists="append", index=False)
dfcwci.to_sql("contributors", db, if_exists="append", index=False)

candidate_cols = [e[1] for e in make_query("PRAGMA table_info(candidates);")]
candidate_cols

contributor_cols = [e[1]
                    for e in make_query("PRAGMA table_info(contributors);")]
contributor_cols

out = make_query("SELECT * FROM candidates;")
# Create a dataframe
print(make_frame(out, legend=candidate_cols).head())
