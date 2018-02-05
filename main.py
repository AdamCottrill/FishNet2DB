'''=============================================================
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

A. Cottrill
=============================================================

'''

import dbfread
import logging
import os
import re


from fn2db import *

#  LAKE HURON
LAKE = 'Huron'
#LAKE = 'Superior'

if LAKE == 'Huron':
    DBASE = 'c:/1work/ScrapBook/lhmu_warehouse.db'
    #FN_DIR = 'C:/1work/ScrapBook/fish_net_sc/SC'
    FN_DIR = 'E:/Fishnet/Data'
else:
    DBASE = 'c:/1work/ScrapBook/lsmu_warehouse.db'
    FN_DIR = 'E:/LakeSuperior/DATA'


proj_pattern = r"[A-Z]{2}\d{2}_([A-Z]|\d){3}$"

logfile = os.path.join(os.path.split(DBASE)[0],
                       'fn2db_{}.log'.format(LAKE))
logging.basicConfig(filename=logfile,level=logging.DEBUG)

#get a list of all directories in FN_DIR - recursively moves down
#the directory structure
directories = [x[0] for x in os.walk(FN_DIR)
               if re.search(proj_pattern, x[0])]

for proj_dir in directories:
    #prj_root = os.path.split(proj_dir)[1]
    file_names = get_dbf_files(proj_dir)
    dbfs = [os.path.join(proj_dir,x) for x in file_names]
    for dbf_file in dbfs:
        table_name = get_dbf_table_name(dbf_file)
        #some dbf files start with numbers - can't happen in real db
        if re.match('[0-9]', table_name):
            #add an 'x' onto those table names
            table_name = 'X' + table_name
        table = read_dbf(dbf_file, encoding='latin1')
        if table is None:
            #if the table is empty, there is nothing to do, check the log
            continue
        else:
            field_names = table.field_names
            field_names.append('DBF_FILE')
        db_tables = get_db_tables(DBASE)
        #check the database for this table:
        if table_name.upper() not in db_tables:
            #if the table does not exist, add it using our fields
            create_table(DBASE, table_name, field_names)
        #check the field names in the database with our current data

        db_fnames = set(get_db_table_fields(DBASE, table_name))

        #replace any field names that happen to be sql keywords
        #field_names = ['YSELECT' if x=='SELECT' else x for x in field_names]
        missing = set(field_names) - db_fnames
        #if there are any missing columns, add them
        if missing:
            for fld in missing:
                add_column(DBASE, table_name, fld)
        try:
            append_dbf(DBASE, table_name, table, dbf_file)
        except:
            logging.warning('Problem appending {}'.format(dbf_file))
    print("Done adding {}".format(proj_dir))



#================================
#
#we missed the DD_PRJ.DBF files because they are above project roots
#these can be added seperately:
#    - search for all DD_PRJ.DBF files
#    - read each of them in,
#    - check the db for an appropriate table
#    - create it if necessary
#    - check columns in db, add them if necessary
#    - insert contents of DD_PRJ.DBF file
#    - repeat

dd_prj_files = []

for path, dirs, files in os.walk(FN_DIR):
    #for filename in fnmatch.filter(files, pattern):
    for filename in files:
        if filename == 'DD_PRJ.DBF':
            dd_prj_files.append(os.path.join(path, filename))

#this is a repeat of the code above
for dbf_file in dd_prj_files:
    table_name = get_dbf_table_name(dbf_file)
    table = read_dbf(dbf_file, encoding='latin1')
    field_names = table.field_names
    field_names.append('DBF_FILE')
    db_tables = get_db_tables(DBASE)
    #check the database for this table:
    if table_name not in db_tables:
        #if the table does not exist, add it using our fields
        create_table(DBASE, table_name, field_names)
    #check the field names in the database with our current data
    db_fnames = set(get_db_table_fields(DBASE, table_name))
    missing = set(field_names) - db_fnames
    #if there are any missing columns, add them
    if missing:
        for fld in missing:
            add_column(DBASE, table_name, fld)
    try:
        append_dbf(DBASE, table_name, table, dbf_file)
    except:
        logging.warning('Problem appending {}'.format(dbf_file))
    print("Done adding {}".format(dbf_file))
