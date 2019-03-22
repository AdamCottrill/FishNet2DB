'''=============================================================
c:/1work/Superior/warehouse/split_to_msaccess.py
Created: 03 Feb 2017 10:39:22


DESCRIPTION:

THis script will be used to create ms access databases that contains
data for specific project types.  THe current LSMU warehouse contain
all of the data from the lake superior fishnet archive all projects,
all tables, all fields.  This script will get the FN* and FR* tables,
split the data into discrete mdb datafiles and drop any fields where
all of the rows are null after the split.

Projects will be split based on the project type encoded in their
project codes:  CF, IA, SC, SD, and SF.

This script


A. Cottrill
=============================================================

'''


import datetime
import pypyodbc
import re
import shutil
import sqlite3

from utils import *




#SRC_DB = "C:/Users/COTTRILLAD/Desktop/Floppy_Disk_Project/004/LHA/FN_Projects.db"
#TRG_MDB = "C:/Users/COTTRILLAD/Desktop/Floppy_Disk_Project/004/FN_Files.accdb"

SRC_DB = "C:/Users/COTTRILLAD/Documents/1work/ScrapBook/AOFRC-334-GEN/FN_Projects.db"
TRG_MDB = "C:/Users/COTTRILLAD/Documents/1work/ScrapBook/AOFRC-334-GEN/AOFRC_334-3.mdb"


connection = pypyodbc.win_create_mdb(TRG_MDB)

#SRC_DB = 'c:/Users/COTTRILLAD/Documents/1work/ScrapBook/aofrc.db'
#FN_DIR = 'C:/1work/ScrapBook/fish_net_sc/SC'

#TRG_MDB = "Y:/File Transfer/ChrisD/AOFRC Data/AOFRC-334-GEN/AOFRC_334.accdb"
#TRG_MDB = "C:/Users/COTTRILLAD/Documents/1work/ScrapBook/Database1.accdb"

TABLE_TYPES = ['FN','FR']

#get all of the  table names that start with FN* or FR*

con = sqlite3.connect(SRC_DB)
cursor = con.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [x[0] for x in cursor.fetchall()]

#prj_cd tables will hold list of tables that contain the field PRJ_CD
#other tables generally seem to be FISHNET design tables.
prj_cd_tables = []
other_tables = []
sql = 'select * from [{}] limit 1;'
for table in tables:
    cursor.execute(sql.format(table))
    flds = [x[0] for x in cursor.description]
    if 'PRJ_CD' in flds:
        prj_cd_tables.append(table)
    else:
        other_tables.append(table)


#connect to ms_access:
constring = r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};'
mdbcon = pypyodbc.connect(constring.format(TRG_MDB))
mdbcur = mdbcon.cursor()
#loop over our project_tables, get the fields in each in our paired
#down database, create the tables in MS_Access and then select all
#records from sqlite and insert them into msaccess.

#now the database should only contain tables with data, and only those
#columns with data should be included in each table.
#use the sqlite data to create a mdb clone.


#drop tables that do not contain the Field PRJ_CD:
#for table in other_tables:
#    print("Dropping {}".format(table))
#    cursor2.execute('DROP TABLE {}'.format(table))
#con2.commit()


sql1 = """DELETE FROM [{}]
      WHERE substr(PRJ_CD, 5, 2) <> '{}';"""

sql2 = """select * from [{}] limit 1"""

sql3 = "select count(*) from [{}] where [{}] IS NOT NULL and [{}]<>'';"

#for each of remaining tables, delete all of the records that are not
# associated with the current project type.  then query each field of
# each table and remove any fields that do not contain any data (creel
# specific fields in offshore index database).  build a list of fields
# with data, starting with our FN key fields then create a tempoary
# table using those fields and populate it with a select query.
# Delete the original table and rename, the new, paired down table to
# its original name.



#get a list of tables in our paired down database:
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
current_tables = [x[0] for x in cursor.fetchall()]

for table in prj_cd_tables:
    if table in current_tables:

        cursor.execute(sql2.format(table))
        flds = [x[0] for x in cursor.description]
        flds2 = [get_fld_fndict(x) for x in flds]
        #flds3 = [format_fld_to_sql(x, True) for x in flds2]
        flds3 = [format_fld_to_sql(x, False) for x in flds2]
        _sql = build_create_table_sql(table, flds3)
        mdbcur.execute(_sql)

        cursor.execute('select * from [{}]'.format(table))
        rs = cursor.fetchall()
        print("inserting data into {}".format(table))
        insert_sql = build_sql_insert(table, flds)
        #mdbcur.executemany(insert_sql, rs)
        for record in rs:
            try:
                record2 = format_record(record,flds2)
                mdbcur.execute(insert_sql, record2)
            except:
                print('oops!', record)

#close our database connections
mdbcon.commit()
mdbcon.close()
cursor.close()
con.close()

print('Done!')
