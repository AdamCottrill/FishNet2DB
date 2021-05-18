def format_record(record, fld_attrs):
    """
    """
    vals = [remove_empty_strings(x) for x in record]
    field_types = [x.get("field_type") for x in fld_attrs]

    for i, x in enumerate(zip(vals, field_types)):
        tmp = x[0]
        if tmp is None:
            next
        elif x[1] == "TIME":
            tmp = convert_to_time(x[0])
            if tmp is None:
                print("TIME", "'{}'".format(x[0]), record)
        elif x[1] == "DATE":
            tmp = convert_to_date(x[0])
            if tmp is None:
                print("DATE", "'{}'".format(x[0]), record)
        elif x[1] == "INTEGER":
            tmp = convert_to_int(x[0])
            if tmp is None:
                print("INTEGER", "'{}'".format(x[0]), record)
        elif x[1] == "FLOAT":
            tmp = convert_to_float(x[0])
            if tmp is None:
                print("FLOAT", "'{}'".format(x[0]), record)
        vals[i] = tmp

    return vals


def remove_empty_strings(x):
    """

    Arguments:
    - `x`:
    """

    if x:
        x = x.strip()

    if x == "" or x == ":" or x == ".":
        return None
    else:
        return x


def convert_to_time(val):
    """
    """
    import datetime

    try:
        return datetime.datetime.strptime(val, "%H:%M").time()
    except:
        try:
            # some people record midnight as 24:00 rather than 00:00
            val = val.replace("24:", "00:")
            return datetime.datetime.strptime(val, "%H:%M").time()
        except:
            None


def convert_to_date(val):
    """
    """

    import re
    import datetime

    regex = r"\d{2}\.\d{2}\.\d{2}"

    if re.match(regex, val):
        date_format = "%y.%m.%d"
    else:
        date_format = "%Y-%m-%d"

    try:
        return datetime.datetime.strptime(val, date_format)
    except:
        return None


def convert_to_int(val):
    """
    """
    try:
        return int(val)
    except:
        return None


def convert_to_float(val):
    """given a string value val try to convert it to a float.  Remove any
    extraniouse spaces or trailing periods that often appear in this field

    """
    try:
        return float(val.replace(" ", "").rstrip("."))
    except:
        return None


def create_table_sql(flds2keep, table):
    """

    Arguments:
    - `flds`:
    - `table`:
    """
    fld_names = ["[{}]".format(x) for x in flds2keep]
    fld_list = ", ".join(fld_names)

    sql = "create table tmp as select {} from [{}];".format(fld_list, table)
    return sql


def order_flds(flds):
    """

    Arguments:
    - `flds`:
    """
    FNKEYS = ["PRJ_CD", "SAM", "EFF", "SPC", "GRP", "FISH", "AGEID", "FOODID"]

    # order the fields so that we have keys first then alphabetically after that:
    key_flds = [x for x in FNKEYS if x in flds]
    data_flds = [x for x in flds if x not in FNKEYS]
    data_flds.sort()
    flds = key_flds + data_flds
    return flds


def sanitize_sql(sql):
    """A little helper function to take sqlite/sqlalchemy table
    definitions and convert them to sql statements that access
    understands.

    Arguments:
    - `sql`: the sql string that needs to be sanitized

    """

    import re

    #  'sqlite':'ms_access'
    sanitize_dict = {
        "\tdate": "\t[date]",
        "BOOLEAN": "YESNO",
        #',( )\n\tCHECK (.)*':'',
        "\n\tCHECK\s*(.)*": "",
        "value": "[value]",
        "\),\s*\n\)": ")\n)",  # trailing comma
    }

    for k, v in sanitize_dict.items():
        sql = re.sub(k, v, sql)

    # removing the trailing comma's doesn't seem to work when we
    # include it in the dictionary:
    sql = re.sub("\),\s*\n\)", ")\n)", sql)

    # access has a limit of 255 for varchar fields.
    pattern = r"VARCHAR\((\d+)\)"
    for m in re.finditer(pattern, sql):
        if int(m.groups()[0]) > 255:
            sql = sql.replace(m.group(), "VARCHAR(255)")

    return sql


def get_fld_fndict(fld_name):
    """a little function to connect the fishnet data dictionary and
    retrieve the attributes of the given field.  If a field matching
    the given field name can be found, it returns a dictionary
    containing the keys: name, type and attr.  If a matching field
    cannot be found, it returns a generic dictionary that includes the
    given name, attribute of none and field type of VARCHAR. (the
    sqlite DB appears to use TEXT for all field types so a default of
    VARCHAR should be ok for now).

    Arguments:
    - `fld_name`:

    """

    import sqlite3

    # hard code for today - throw away code anyway.
    # db = "X:/flask/ProcValQE/db/ProcValQueries.db"
    # db = "C:/1work/Python/PyPV/build_orig/exe.win32-3.4/PVQueries.db"
    # db = "C:/Users/cottrillad/documents/1work/Python/flask/ProcValQE/db/ProcValQueries.db"
    db = "C:/Users/COTTRILLAD/1work/Python/FN2db/ProcValQueries.db"

    conn = sqlite3.connect(db)
    cur = conn.cursor()

    sql = """Select upper(field_name) as field_name, field_type, field_attr
             from field
             where field_name=upper(?) and field_type is not NULL;"""

    cur.execute(sql, (fld_name,))
    rs = cur.fetchall()

    if rs:
        record_count = len(rs)
        if record_count > 1:
            msg = "WARNING! {} records returned.  Only the first field used."
            print(msg.formart(record_count))

        fld = rs[0]
        col_names = [x[0] for x in cur.description]
        return {k: v for k, v in zip(col_names, fld)}
    else:
        return {"field_attr": None, "field_name": fld_name, "field_type": "VARCHAR"}


def format_fld_to_sql(fld_dict, all_varchar=False):
    """given an dictionary containing the attributes of a field, return a
    string containing the sub-string that will be used to include that
    field in a CREATE TABLE statement.

    the string is returned as:

    '[{field_name}] {field_type}({field_attr})' OR
    '[{field_name}] {field_type}'

    Depending on field type and where or not a lenght attribute is present.

    Field names are wrapped in [] to allow for spaces or other oddities.

    Arguments:
    - `fld_dict`:

    """
    if fld_dict.get("field_attr") == "memo":
        fld_dict["field_attr"] = None
        fld_dict["field_type"] = "MEMO"

    # convert to integers afterwards in MS Access - don't want to loose data here
    if fld_dict.get("field_type") == "INTEGER":
        fld_dict["field_type"] = "FLOAT"

    if all_varchar:
        if fld_dict["field_type"] == "MEMO":
            base = "[{field_name}] MEMO"
        else:
            base = "[{field_name}] VARCHAR"

    else:
        fields_with_size = ["VARCHAR", "TEXT"]

        size_type = True if fld_dict.get("field_type") in fields_with_size else False
        has_attr = True if fld_dict.get("field_attr") else False

        if has_attr and size_type:
            base = "[{field_name}] {field_type}({field_attr})"
        else:
            base = "[{field_name}] {field_type}"

    return base.format(**fld_dict)


def build_create_table_sql(table, field_desc):
    """ give a list of sql-formatted fields of the form:
   '[field_name] field_type', make the create table sql statement:

    Arguments:
    - `table`:
    - `field_desc`:
    """

    field_part = ",\n".join(field_desc)
    base = "CREATE TABLE [{}] ({});"
    return base.format(table, field_part)


def build_sql_insert(table, flds):
    """

    Arguments:
    - `table`:
    - `flds`:
    """

    sql_base = "insert into {} ({}) values ({})"
    # escape and concatenate fields
    flds_str = ",\n".join(["[{}]".format(x) for x in flds])
    # create a parameter list - one ? for each field
    args = ",".join(len(flds) * ["?"])

    return sql_base.format(table, flds_str, args)
