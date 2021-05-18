"""=============================================================
 c:/Users/COTTRILLAD/1work/Python/FN2db/merge_dbs.py
 Created: 17 May 2021 19:34:43

 DESCRIPTION:

  Rather than re-reading all of hte fishnet archives and inserting
  them into a single db, take the pre-comiled databases for each lake
  and merge theminto a grand-grandwazoo.




 A. Cottrill
=============================================================

"""
import subprocess
import shutil
import sqlite3

from fn2db import add_column, add_column_sql


def run_query(db, sql, fetchone=False):
    """

    Arguments:
    - `sql`:
    """
    data = {}
    with sqlite3.connect(db) as con:
        cursor = con.cursor()
        cursor.execute(sql)
        if fetchone:
            data = cursor.fetchone()
        else:
            data = cursor.fetchall()
    return data


def copy_table(src, trg, tablename):
    """

    Arguments:
    - `src`:
    - `trg`:
    """
    # cmd = 'sqlite3 "{}" .dump [{}] > sqlite3 "{}"'.format(src, tablename, trg)
    # subprocess.run(cmd)

    attach = "ATTACH DATABASE '{}' as 'src';".format(src)
    insert = "INSERT INTO {0} SELECT * FROM src.{0};".format(tablename)

    with sqlite3.connect(trg) as con:
        cursor = con.cursor()
        cursor.execute(attach)
        cursor.execute(insert)
        con.commit()


def get_table_definition(db, tablename):
    """

    Arguments:
    - `db`:
    - `tablename`:
    """
    sql = "SELECT sql FROM sqlite_master WHERE name = '{}';".format(tablename)

    with sqlite3.connect(db) as con:
        cursor = con.cursor()
        cursor.execute(sql)
        createsql = cursor.fetchone()
    return createsql[0]


def make_table(src, trgdb, tablename):
    """

    Arguments:
    - `trgdb`:
    - `createsql`:
    """

    sql = get_table_definition(src, tablename)

    with sqlite3.connect(trgdb) as con:
        cursor = con.cursor()
        cursor.execute(sql)
        con.commit()


def get_tables(db):
    """Get the known table names for this database"""
    sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    data = run_query(db, sql)
    if data:
        return [x[0] for x in data]
    else:
        return []


def get_field_names(db, table_name):
    """Get the known field names for this table.  This could be a cache some day."""
    # sql = "SELECT * FROM [{}]".format(table_name)
    sql = "PRAGMA table_info( [{}] );".format(table_name)

    data = run_query(db, sql)
    if data:
        return [x[1] for x in data]
    else:
        return []


def insert_records(trg, src, table, src_fields):
    # append the data

    columns = ", ".join(["[{}]".format(x) for x in src_fields])

    attach = "ATTACH DATABASE '{}' as 'src';".format(src)
    insert = "INSERT INTO [{0}]({1}) SELECT {1} FROM src.[{0}];".format(table, columns)
    # detach = "DETATCH DATABASE 'src';"

    with sqlite3.connect(trg) as con:
        cursor = con.cursor()
        cursor.execute(attach)
        cursor.execute(insert)
        # cursor.execute(detach)


# =========================================================


dbs = [
    "C:/Users/COTTRILLAD/1work/ScrapBook/lhmu_warehouse.db",
    "C:/Users/COTTRILLAD/1work/ScrapBook/lsmu_warehouse.db",
    "F:/LOntario_data/LOMU_Projects.db",
    "F:/LOntario_data/LOMU_Projects2.db",
]
# copy the first datbase to the target:

TRGDB = "C:/Users/COTTRILLAD/1work/ScrapBook/GrandGrandWazoo/GrandGrandWazoo.db"

shutil.copyfile(dbs[0], TRGDB)

for src in dbs[1:]:
    print("appending ", src)
    # get the tables from the second database and compare to the first
    src_tables = get_tables(src)
    trg_tables = get_tables(TRGDB)

    for table in src_tables:
        if table not in trg_tables:
            # if the table is missing entirely insert the table from src:
            print("copying table {}".format(table))
            make_table(src, TRGDB, table)

        # compare the fields in each table, make sure that all of the fields exist
        src_fields = get_field_names(src, table)
        trg_fields = get_field_names(TRGDB, table)

        # add any missing fields:
        missing = list(set(src_fields) - set(trg_fields))
        if missing:
            for fld in missing:
                add_column(TRGDB, table, fld)

        # insert the data from our source database
        print("\t{}".format(table))
        insert_records(TRGDB, src, table, src_fields)


# next table
# next database.
