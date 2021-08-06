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

from fn2db import (
    add_column,
    add_column_sql,
    get_table_definition,
    make_table,
    copy_table,
    run_query,
    get_tables,
    get_fields_names
    insert_records
)


# =========================================================


dbs = [
    "C:/Users/COTTRILLAD/1work/ScrapBook/lhmu_warehouse.db",
    "C:/Users/COTTRILLAD/1work/ScrapBook/lsmu_warehouse.db",
    "F:/LOntario_data/LOMU_Projects.db",
    "F:/LOntario_data/LOMU_Projects2.db",
    "F:/LErie_data/parntership/LEMU_Projects.db",
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
