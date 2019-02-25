# script which reads data from zip files containing json documents
# and add the data to a PostgreSQL database

import getopt
import sys
import os
import json
import zipfile
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import AsIs



def print_help():
    """Print help text."""
    help_text = \
    """Converts JSON documents compressed in ZIP files into a PostgreSQL database.

    python moving_data.py -d[database] -f[file] -h

    Please provide options for database and file.

    -d, --database
    Name of the database.

    -f, --file
    Name of the ZIP file containing data.

    -h, --help
    Display help."""

    print(help_text)
    return None

def get_args():
    """Get arguments passed at the command line."""
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'd:f:h',
                ['database=', 'file=', 'help'])

    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)

    if len(args) > 0:
        print("""This function does not take arguments outside options.
Please make sure you did not forget to include an option name.""")
        sys.exit()

    db_name = None
    file_path = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()
        elif opt in ('-d', '--database'):
            db_name = arg
        elif opt in ('-f', '--file'):
            file_path = arg
        else:
            print('Unhandled option')

    if db_name == None:
        print('Please provide a database name.')
        sys.exit()
    if file_path == None:
        print('Please provide an input file path.')
        sys.exit()

    return db_name, file_path

def connect_db(db_name):
    """Connect to PostgreSQL database."""
    user = os.getenv('POSTGRES_USER', 'default')
    passw = os.getenv('POSTGRES_PASS', 'default')

    try:
        conn = psycopg2.connect(user=user, password=passw, dbname=db_name)
    except:
        print("Cannot connect to database")
        sys.exit()

    return conn

def tb_exists(tb_name, conn):
    """Return True if table exists, else False."""

    check_table_sql = 'SELECT 1 FROM pg_tables WHERE tablename = %s;'

    cur = conn.cursor()
    cur.execute(check_table_sql, (tb_name,))

    if cur.fetchone() == None:
        return False

    return True

def open_zipfile(file_path):
    """Return an open zipfile."""

    if zipfile.is_zipfile(file_path):
        data = zipfile.ZipFile(file_path)

    else:
        print('The file provided is not a valid zip file.')
        sys.exit()

    return data

def json_to_list(json_doc):
    """Return orders list from the content of a zipped JSON document containing \
a single dictionary of structure {'orders': [<list items>]}"""

    doc_content = json_doc.read()
    dic = json.loads(doc_content)

    orders_list = dic['orders']

    return orders_list

def get_fields(dic):
    """Get PostgreSQL table field names from a dictionary."""

    # orders_list is a list of dictionaries where keys are fields name
    fields = [k for k in dic.keys()]

    return fields

def create_tb(tb_name, fields_type, conn):
    """Return a string that creates a table."""

    # SQL identifier to protect against SQL injection
    sql_ids = [sql.Identifier(tb_name)]

    # executable string to create table
    exec_str_tb = "CREATE TABLE {} ("

    for field, field_type in fields_type.items():
        exec_str_tb += "{} {}, ".format(field, field_type)

    exec_str_tb = exec_str_tb[:-2] + ");"

    cur = conn.cursor()
    cur.execute(sql.SQL(exec_str_tb).format(*sql_ids))
    conn.commit()

def insert_dic(tb_name, dic, conn):
    """Insert dictionary into PostgreSQL table."""

    sql_ids = [sql.Identifier(tb_name)]

    fields = ', '.join(dic.keys())

    values = []
    for v in dic.values():
        if v == '':
            v = None
        values.append(v)

    exec_str_insert = 'INSERT INTO {} (%s) VALUES %s'
    exec_str = sql.SQL(exec_str_insert).format(*sql_ids)
    cur = conn.cursor()
    cur.execute(exec_str, (AsIs(fields), tuple(values)))
    conn.commit()

def main():
    """Main function."""

    # fields type 
    main_fields_type = {
            'id': 'BIGINT',#1
            'email': 'VARCHAR(40)',#2
            'closed_at': 'DATE',#3
            'created_at': 'DATE',#4
            'updated_at': 'DATE',#5
            'number': 'INT',#6
            'note': 'VARCHAR(40)',#7
            'token': 'VARCHAR(40)',#8
            'gateway': 'VARCHAR(40)',#9
            'test': 'BOOLEAN',#10
            'total_price': 'FLOAT8',#11
            'subtotal_price': 'FLOAT8',#12
            'total_weight': 'INT',#13
            'total_tax': 'FLOAT8',#14
            'taxes_included': 'BOOLEAN',#15
            'currency': 'CHAR(3)',#16
            'financial_status': 'VARCHAR(40)',#17
            'confirmed': 'BOOLEAN',#18
            'total_discounts': 'FLOAT8',#19
            'total_line_items_price': 'FLOAT8',#20
            'cart_token': 'VARCHAR(40)',#21
            'buyer_accepts_marketing': 'BOOLEAN',#22
            'name': 'VARCHAR(40)',#23
            'referring_site': 'VARCHAR(40)',#24
            'landing_site': 'VARCHAR(40)',#25
            'cancelled_at': 'VARCHAR(40)',#26
            'cancel_reason': 'VARCHAR(40)',#27
            'total_price_usd': 'FLOAT8',#28
            'checkout_token': 'VARCHAR(40)',#29
            'reference': 'VARCHAR(40)',#30
            'user_id': 'VARCHAR(40)',#31
            'location_id': 'VARCHAR(40)',#32
            'source_identifier': 'VARCHAR(40)',#33
            'source_url': 'VARCHAR(40)',#34
            'processed_at': 'DATE',#35
            'device_id': 'VARCHAR(10)',#36
            'phone': 'VARCHAR(40)',#37
            'customer_locale': 'VARCHAR(40)',#38
            'app_id': 'VARCHAR(40)',#39
            'browser_ip': 'VARCHAR(15)',#40
            'landing_site_ref': 'VARCHAR(40)',#41
            'order_number': 'VARCHAR(40)',#42
            'processing_method': 'VARCHAR(40)',#43
            'checkout_id': 'VARCHAR(40)',#44
            'source_name': 'VARCHAR(40)',#45
            'fulfillment_status': 'VARCHAR(40)',#46
            'tags': 'VARCHAR(40)',#47
            'contact_email': 'VARCHAR(40)',#48
            'order_status_url': 'VARCHAR(40)',#49
            'total_discount': 'FLOAT8'#50
    }

    # fields type for line_items
    item_fields_type = {
            'id': 'VARCHAR(40)',
            'quantity': 'INT',
            'variant_id': 'VARCHAR(40)',
            'product_id': 'VARCHAR(40)'
    }


    # get command line arguments
    db_name, file_path = get_args()
    file_path = '../input/' + file_path

    # connect to database
    conn = connect_db(db_name)

    # open zip file
    data = open_zipfile(file_path)

    # loop over archived JSON documents and convert to a list of orders
    for name in data.namelist():

        json_doc = data.open(name)

        orders_list = json_to_list(json_doc)

        # create table if they do not exist
        if not tb_exists('orders', conn):
            create_tb('orders', main_fields_type, conn)
        if not tb_exists('items', conn):
            create_tb('items', item_fields_type, conn)

        # append orders and items to respective tables
        for order in orders_list:
            # get items list
            items_list = order.pop('line_items')

            # get fields for order
            order_fields = get_fields(order)
            insert_dic('orders', order, conn)

            # loop over items
            for item in items_list:
                item_fields = get_fields(item)
                insert_dic('items', item, conn)

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    main()

