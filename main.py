"""=============================================================
c:/1work/Python/FN2db/main.py
Created: 16 Dec 2014 14:35:49

DESCRIPTION:

This script uses a number of function contained in fn2db.py to compile
all of the dbf files associated with a fishnet project into a single
sqlite database.  All dbf files tables in the directory are imported
or appended into a table with the same base name (eg. FN011.dbf is
appened to a table called FN011 in the database).  If there are fields
in the dbf that are not in the target table, they will be added to the
target table.  One side effect of this approach is that the fields
names in the merged data base will be the union of all field names for
all of the dbf files with that name.  Post processing will be need to
break the database into logical chunks (e.g by program area) followed
by removal columns that are unused in those projects.  For example,
fields specific to creels do no need to be included in the commercial
catch sampling database.

Usage:

  > python main.py --src_dir=<path_to_your_archive>

  > python main.py --src_dir=<path_to_your_archive> --unzip=True

A. Cottrill
=============================================================

"""

import argparse
import logging
import os
import re
import sqlite3
import zipfile

import pypyodbc
from dbfread.exceptions import MissingMemoFile

# our utilities
import fn2db
import sqlite2mdb

# TODO - make these command line arguments.
parser = argparse.ArgumentParser()
parser.add_argument("--src_dir", "-src", help="Directory containing the FN Archives")
parser.add_argument("--unzip", "-z", help="Unzip zipped archives. Defaults to False")
parser.add_argument(
    "--verbose", "-v", help="Recreate Target Database.  Defaults to True"
)
parser.add_argument(
    "--append",
    "-a",
    help="Append contents of D to existing database.  Defaults to False",
)

args = parser.parse_args()
SRC_DIR = args.src_dir
UNZIP = args.unzip if args.unzip else False
VERBOSE = args.unzip if args.unzip else True
APPEND = args.unzip if args.unzip else False


DBASE = os.path.join(SRC_DIR, "FN2_Projects.db")
TRG_MDB = os.path.join(SRC_DIR, "FN2_Projects.accdb")

logfile = os.path.join(os.path.split(DBASE)[0], "fn2db.log")
logging.basicConfig(filename=logfile, level=logging.DEBUG)


#  ===========================================
#                 UNZIP

if UNZIP:
    for path, dir_list, file_list in os.walk(SRC_DIR):
        for file_name in file_list:
            if file_name.lower().endswith(".zip"):
                abs_file_path = os.path.join(path, file_name)

                parent_path = os.path.split(abs_file_path)[0]
                output_folder_name = os.path.splitext(abs_file_path)[0]
                output_path = os.path.join(parent_path, output_folder_name)
                if VERBOSE:
                    pretty_fname = abs_file_path.replace(
                        os.path.split(SRC_DIR)[0], "~"
                    ).replace("/", "\\")
                    print("Unzipping {}".format(pretty_fname))
                zip_obj = zipfile.ZipFile(abs_file_path, "r")
                zip_obj.extractall(output_path)
                zip_obj.close()
    if VERBOSE:
        print("Done unzipping archives....")


#  ===========================================
#             DBF -> SQLite

# compiling fishnet *.dbf files into sqlite database.

if VERBOSE:
    print("Reading dbf files....")

# proj_pattern = r"[A-Z]{2}\d{2}_([A-Z]|\d){3}$"
# match this SC97__1F or this: LEM/SC97_01F
proj_pattern = r"[A-Z]{2}\d{2}_(_|[A-Z]|\d)([A-Z]|\d){2}$"

# get a list of all directories in SRC_DIR - recursively moves down
# the directory structure
directories = [x[0] for x in os.walk(SRC_DIR) if re.search(proj_pattern, x[0])]

for proj_dir in directories:

    if os.path.isdir(os.path.join(proj_dir, "DATA")):
        proj_dir = os.path.join(proj_dir, "DATA")

    # prj_root = os.path.split(proj_dir)[1]
    file_names = fn2db.get_dbf_files(proj_dir)
    dbfs = [os.path.join(proj_dir, x) for x in file_names]

    for dbf_file in dbfs:
        table_name = fn2db.get_dbf_table_name(dbf_file)
        # some dbf files start with numbers - can't happen in real db
        if re.match("[0-9]", table_name):
            # add an 'x' onto those table names
            table_name = "X" + table_name

        try:
            table = fn2db.read_dbf(dbf_file, encoding="latin1")
        except:
            print("Missing MemoFile: {}".format(dbf_file))
            continue

        if table is None:
            # if the table is empty, there is nothing to do, check the log
            continue
        else:
            field_names = table.field_names
            field_names.append("DBF_FILE")
        db_tables = fn2db.get_db_tables(DBASE)
        # check the database for this table:
        if table_name.upper() not in db_tables:
            # if the table does not exist, add it using our fields
            fn2db.create_table(DBASE, table_name, field_names)
        # check the field names in the database with our current data
        db_fnames = set(fn2db.get_db_table_fields(DBASE, table_name))

        # replace any field names that happen to be sql keywords
        # field_names = ['YSELECT' if x=='SELECT' else x for x in field_names]
        missing = set(field_names) - db_fnames
        # if there are any missing columns, add them
        if missing:
            for fld in missing:
                fn2db.add_column(DBASE, table_name, fld)
        try:
            fn2db.append_dbf(DBASE, table_name, table, dbf_file)
        except:
            logging.warning("Problem appending {}".format(dbf_file))


# ================================
#
# we missed the DD_PRJ.DBF files because they are above project roots
# these can be added seperately:
#    - search for all DD_PRJ.DBF files
#    - read each of them in,
#    - check the db for an appropriate table
#    - create it if necessary
#    - check columns in db, add them if necessary
#    - insert contents of DD_PRJ.DBF file
#    - repeat

dd_prj_files = []

for path, dirs, files in os.walk(SRC_DIR):
    # for filename in fnmatch.filter(files, pattern):
    for filename in files:
        if filename == "DD_PRJ.DBF":
            dd_prj_files.append(os.path.join(path, filename))

# this is a repeat of the code above but for dd_prj files (whatever they are).
if dd_prj_files:
    for dbf_file in dd_prj_files:
        table_name = fn2db.get_dbf_table_name(dbf_file)
        table = fn2db.read_dbf(dbf_file, encoding="latin1")
        if table is None:
            logging.warning(f"Problem reading {table_name} in {dbf_file}")
        else:
            field_names = table.field_names
            field_names.append("DBF_FILE")
            db_tables = fn2db.get_db_tables(DBASE)
            # check the database for this table:
            if table_name not in db_tables:
                # if the table does not exist, add it using our fields
                fn2db.create_table(DBASE, table_name, field_names)
            # check the field names in the database with our current data
            db_fnames = set(fn2db.get_db_table_fields(DBASE, table_name))
            missing = set(field_names) - db_fnames

            # if there are any missing columns, add them
            if missing:
                for fld in missing:
                    fn2db.add_column(DBASE, table_name, fld)
            try:
                fn2db.append_dbf(DBASE, table_name, table, dbf_file)
            except:
                logging.warning(f"Problem appending {dbf_file}")

if VERBOSE:
    print("Done reading dbf files....")

#  ===========================================
#             SQLite -> MS Access

# get a list of our tables in the SQLite database:

con = sqlite3.connect(DBASE)
cursor = con.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [x[0] for x in cursor.fetchall()]

# prj_cd tables will hold list of tables that contain the field PRJ_CD
# other tables generally seem to be FISHNET design tables.
prj_cd_tables = []
other_tables = []
sql = "select * from [{}] limit 1;"
for table in tables:
    cursor.execute(sql.format(table))
    flds = [x[0] for x in cursor.description]
    if "PRJ_CD" in flds:
        prj_cd_tables.append(table)
    else:
        other_tables.append(table)

# NOTE: this could be conditional - all tables or just data-tables?
# drop tables that do not contain the Field PRJ_CD:
for table in other_tables:
    print("Dropping {}".format(table))
    cursor.execute("DROP TABLE [{}]".format(table))
con.commit()


# If our target database exists - connect to it, if it doesn't, create it.
constring = r"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};"
if not os.path.isfile(TRG_MDB):
    mdbcon = pypyodbc.win_create_mdb(TRG_MDB)

mdbcon = pypyodbc.connect(constring.format(TRG_MDB))
mdbcur = mdbcon.cursor()


if APPEND is False:
    mdb_tables = [
        x.get("table_name") for x in mdbcur.tables() if x.get("table_type") == "TABLE"
    ]
    print("Deleting existing database tables...")
    # clear out any existing tables - ms access does not support 'drop if exists'
    # start with tables at the bottom of our relationship heirarchy:
    for table in mdb_tables:
        print("\tDeleting {}".format(table))
        mdbcur.execute("DROP TABLE [{}];".format(table))

# for each of remaining tables, query each field of
# each table and remove any fields that do not contain any data (creel
# specific fields in offshore index database).  build a list of fields
# with data, starting with our FN key fields then create a tempoary
# table using those fields and populate it with a select query.
# Delete the original table and rename, the new, paired down table to
# its original name.

for table in prj_cd_tables:

    sql = "select * from [{}] limit 1"
    cursor.execute(sql.format(table))
    flds = [x[0] for x in cursor.description]
    flds_dict = [sqlite2mdb.get_fld_fndict(x) for x in flds]

    if APPEND is False:
        # make the tables
        # flds3 = [format_fld_to_sql(x, True) for x in flds_dict]
        flds3 = [sqlite2mdb.format_fld_to_sql(x, False) for x in flds_dict]
        _sql = sqlite2mdb.build_create_table_sql(table, flds3)
        print(f"Creating {table}")
        mdbcur.execute(_sql)

    cursor.execute("select * from [{}]".format(table))
    rs = cursor.fetchall()

    print("inserting data into {}".format(table))
    insert_sql = sqlite2mdb.build_sql_insert(table, flds)
    # mdbcur.executemany(insert_sql, rs)
    for record in rs:
        try:
            record2 = sqlite2mdb.format_record(record, flds_dict)
            mdbcur.execute(insert_sql, record2)
        except:
            msg = f"Inserting into access {table} - {','.join(record2)}"
            logging.warning(msg)
    mdbcon.commit()

# close our database connections
mdbcon.commit()
mdbcon.close()
cursor.close()
con.close()

print("Done!")
