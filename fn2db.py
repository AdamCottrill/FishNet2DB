'''=============================================================
c:/1work/Python/FN2db/FN2db.py
Created: 16 Dec 2014 14:35:20


DESCRIPTION:

This script contains a number of functions that are used to read,
process and append fishnet (dbf) files into a sqlite database.

A. Cottrill
=============================================================

'''


def get_dbf_files(prj_dir):
    """given the project directory, return the names of all of the dbf
    files in the directory.

    Arguments:
    - `prj_dir`:

    """
    import os

    dbf_files = []
    for fname in os.listdir(prj_dir):
        if fname.upper().endswith('.DBF'):
            dbf_files.append(fname)
    return dbf_files


def get_db_tables(db):
    """given a database, return a list of tables it contains
    """
    import sqlite3
    con = sqlite3.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        rows = cur.fetchall()
    tables = [x[0].upper() for x in rows]
    return tables



def clear_db(db):
    """clear all of the tables out of a database.  CAREFUL! No warning is
    given and the result is irreversible.

    """
    tables = get_db_tables(db)
    sql_base = "drop table [{}]"
    import sqlite3
    con = sqlite3.connect(db)
    with con:
        cur = con.cursor()
        for tbl in tables:
            sql =sql_base.format(tbl)
            cur.execute(sql)
        con.commit()
    return None


def get_dbf_table_name(dbf_path):
    """Given a path to a dbf file, extract the name of the associated
    database table by stripping off the path components and file extension.

    Arguments:
    - `dbf_path`:

    """
    import os

    tbl_name = os.path.split(dbf_path)[1]
    #make sure it is always uppercase and strip off the extension
    tbl_name = tbl_name.upper().replace('.DBF','')

    return tbl_name

def create_table(db, table_name, field_names):
    """given a database, a table, and the associated field names, create
    the table with those fields in the provided database.  If the
    field name is 'SELECT' it is replaced with 'XSELECT'.  Select is a
    sql keyword that causes trouble as a field name.
    """

    import sqlite3
    con = sqlite3.connect(db)
    sql = create_table_sql(table_name, field_names)
    #sql = sql.replace('SELECT','YSELECT')
    with con:
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
    return None

def create_table_sql(table_name, field_names):
    """build the sql string that will create the table named <table_name>
    with the appropriate columns.  Field and table names are wrapped
    in square brackets just incase they have spaces in them.

    Arguments:
    - `dbf_path`:
    - `field_names`:

    """
    #build the sql string associated with field names and types
    #(all currently varchars)
    fld_names = ', '.join(['[{}] varchar'.format(x) for x in field_names])

    sql_base = 'create table [{0}] ({1})'
    sql = sql_base.format(table_name, fld_names).upper()

    return(sql)

#jj = create_table_sql(tbl_name, field_names)

def add_column_sql(table, column, column_def='varchar'):
    """build the sql string that will add a column named <column> to
    table named <table>

    Arguments:
    - `table`:
    - `column`:
    - `column_def`:

    """
    sql_base = 'alter table [{0}] add column [{1}] {2}'
    sql = sql_base.format(table, column, column_def).upper()
    #sql = sql.replace('SELECT','YSELECT')

    return(sql)

#jj = add_column_sql('FN123', 'source')


def read_dbf(dbf_file, encoding='latin1'):
    """a wrapper for dbfread.read that will attempt to read a *.dbt file
    if it can't find a *.fpt file.  the defaualt behaviour or dbfread.read
    is to throw and error without any alternative.'

    Arguments:
    - `dbf_file`: full path to the pdf file to be read
    - `encoding`: the encoding type passed to dbfread.read
    """

    import dbfread
    import shutil
    import os
    import logging

    try:
        table = dbfread.DBF(dbf_file, encoding='latin1')
    except IOError:
        #see if there is a DBT file if so, rename it to fpt and try again
        dbt = dbf_file.replace('.DBF', '.DBT')
        fpt= dbf_file.replace('.DBF', '.FPT')
        if os.path.isfile(dbt):
            shutil.copy(dbt, fpt)
        else:
            #the missing file isn't the problem'
            logging.warning('Unable to find memo file for {}'.format(dbf_file))
            return None
        try:
            table = dbfread.DBF(dbf_file, encoding='latin1')
        except:
            logging.warning('Unable to read memo file for {}'.format(dbf_file))
            return None
        #finally:
        #    if os.path.isfile(fpt):
        #        os.remove(fpt)
    return table


#def read_dbf0(dbf_file):
#    """
#    Arguments:
#    - `dbf_file`:
#    """
#    import dbf
#
#    try:
#        table = dbf.Table(dbf_file)
#        table.open()
#    except:
#        pass
#    return table
#
#


def add_column(db, table, column, column_def='varchar'):
    """add a column named <column> to the table <table> in the specified
    sqlite database.  columns are varchar unless specified otherwise.

    """
    sql = add_column_sql(table, column, column_def)
    import sqlite3
    con = sqlite3.connect(db)
    with con:
        cur = con.cursor()
        cur.execute(sql)
    con.commit()



def get_db_table_fields(db,table):
    """given a database path and a table name, return a list of the field names
    in the table.

    Arguments:
    - `db`: full path to sqlite database
    - `table`: which table do want the names for?

    """
    import sqlite3
    con = sqlite3.connect(db)
    with con:
        cur = con.cursor()
        cur.execute("pragma table_info([{}])".format(table))
        rows = cur.fetchall()
    fields = [x[1] for x in rows]
    return fields



def append_dbf(database, table_name, table, dbf_file):
    """given values from a dbf table, insert them into the database

    Arguments:
    - `database`:
    - `table`:
    """

    import sqlite3
    #now iterate over the dbf table and insert it into our database:
    conn = sqlite3.connect(database)
    with conn:
        cur = conn.cursor()
        for row in table:
            try:
                flds0 = [x for x in row.keys()]
                values = [x for x in row.values()]
            except:
                break

            flds0.append('dbf_file')
            values.append(dbf_file)
            #wrap the field names in square brackets and glue them together
            flds = ', '.join(['[{}]'.format(x) for x in flds0]).upper()
            #flds = flds.replace('SELECT', 'YSELECT')
            qs = ', '.join('?' * len(values))
            sql = """ insert into [{0}] ({1}) values ({2})""".format(table_name,
                                                                   flds, qs)
            cur.execute(sql, values)
    conn.commit()
    conn.close()
