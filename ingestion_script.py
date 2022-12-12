import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime
import io
import time
from multiprocessing import Process, Pool
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logger = logging
logger.basicConfig(filename='app3.log', filemode='w+', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger.info("Starting")

def isAlphab(c):
    return ((ord('a')<=ord(c)<=ord('z'))
            or (ord('A')<=ord(c)<=ord('Z')))


def isNum(c):
    return (ord('0')<=ord(c)<=ord('9'))


def split_name_futures(name):
    symbol = ""
    num_yr = ""
    list_of_symbols = []
    i = 0
    while i in range(len(name)):
        if isAlphab(name[i]):
            symbol += name[i]
            i += 1
            if num_yr != "":
                list_of_symbols.append(num_yr)
                num_yr = ""
        elif isNum(name[i]):
            num_yr += name[i:i + 5]
            i = i + 5
            logger.info(num_yr)
            num_yr = datetime.strptime(num_yr, "%y%b").strftime("%Y-%m-%d")
            if symbol != "":
                list_of_symbols.append(symbol)
                symbol = ""
        else:
            i = i+1
            pass

    list_of_symbols.append(symbol)
    return list_of_symbols


def split_name_options(name):
    symbol = ""
    num = ""
    list_of_symbols = []
    for i in range(len(name)):
        if isAlphab(name[i]):
            symbol += name[i]
            if num != "":
                list_of_symbols.append(num)
                num = ""
        elif isNum(name[i]):
            num += name[i]
            if len(num) == 6:
                num = datetime.strptime(num, "%y%m%d").strftime("%Y-%m-%d")
                list_of_symbols.append(num)
                num = ""
            if symbol != "":
                list_of_symbols.append(symbol)
                symbol = ""

    list_of_symbols.append(symbol)
    return list_of_symbols


def name_breakup(file):
    file_name = file.split('.csv')[0]
    return file_name.split('_')


def split_file_names(file):
    #file_name_breakup = name_breakup(file)
    file_name_breakup = file.split('.csv')[0]
    if file_name_breakup[-2:] == 'UT':
        name_split_var = split_name_futures(file_name_breakup)
    elif file_name_breakup[-2:] == 'PE' or file_name_breakup[-2:] == 'CE':
        name_split_var = split_name_options(file_name_breakup)
    return name_split_var

def create_df(file, columns, engine, output_columns, table_name, base_loc=''):
    # complete_df = pd.DataFrame()
    file_path = base_loc + '/' + file
    split_file_name = split_file_names(file)
    df = pd.read_csv(file_path, header=None, engine='pyarrow')
    df.columns = columns
    #df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    #df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
    df['timestamp'] = pd.to_datetime(df['date'].astype('str') + ' ' + df['time'].astype('str'))
    df['symbol'] = split_file_name[0]
    df['expiry_date'] = split_file_name[1]
    df = df.drop(['date','time'], axis=1)
    df = df[output_columns]
    logger.info(file)
    #df.to_csv(str(write_loc) + '/' + file) 
    #push_to_psql(df, table_name, engine)
    return df

def push_to_psql(df, table_name, engine, schema='public'):
    try:
        # drops old table and creates new empty table   
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        cur.copy_from(output, table_name, null="") # null values become ''
        conn.commit()
        logger.info("Inserted Successfully")
        conn.close()
    except Exception as e:
        logger.info(e)


def drop_table_func(table_name):
    try:
        # Start a PostgreSQL database session
        psqlCon = psycopg2.connect("dbname=tbde_test user=tbde password=password")
        psqlCon.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # Open a database cursor
        psqlCursor = psqlCon.cursor()
        # Name of the table to be deleted
        # Form the SQL statement - DROP TABLE
        dropTableStmt = "DROP TABLE %s;" % table_name
        # Execute the drop table command
        psqlCursor.execute(dropTableStmt)
        # Free the resources
        psqlCursor.close()
        logger.info("Table drop successful")
    except:
        pass


def create_table_func(columns, table_name, schema, time_columns):
    try:
        # Start a PostgreSQL database session
        df = pd.DataFrame(columns=columns)
        try:
            df.head(0).to_sql(table_name, con=engine, schema=schema, if_exists='append',index=False)
        except Exception as e:
            logger.info(e)
        conn = engine.raw_connection()
        psqlCursor = psqlCon.cursor()
        # Name of the table to be deleted
        # Form the SQL statement - DROP TABLE
        dropTableStmt = "SELECT create_hypertable('{0}','{1}');".format(table_name,time_columns)
        # Execute the drop table command
        psqlCursor.execute(dropTableStmt)
        # Free the resources
        psqlCursor.close()
        logger.info("Table drop successful")
    except:
        pass



def main(file):
    #base_loc = '/Users/abhinavrai/Downloads/Sample Data 2'
    base_loc = '/home/tbde/truedata-historical-data/JAN-22-Testing'

    columns = [
        "date",
        "time",
        "ltp",
        "ltq",
        "oi",
        "bid",
        "bid_qty",
        "ask",
        "ask_qty"]

    # complete_df = create_df(list_of_files=list_of_files, columns=columns, output_columns=op_columns,base_loc=base_loc)


    op_columns = [
        "symbol",
        "timestamp",
        "ltp",
        "ltq",
        "oi",
        "bid",
        "bid_qty",
        "ask",
        "ask_qty",
        "expiry_date"]

    df = create_df(file=file, columns=columns, engine=engine, output_columns=op_columns, table_name='company_temp_3', base_loc=base_loc)
    #time.sleep(0.8)



if __name__ == "__main__":
    tic = time.perf_counter()
    table_name = 'company_temp_3'
    drop_table_func(table_name)
    base_loc = '/home/tbde/truedata-historical-data/JAN-22-Testing'
    write_loc = '/home/tbde/truedata-historical-data/JAN-22-Testing-write'
    list_of_files = os.listdir(base_loc)
    engine = create_engine('postgresql://tbde:password@localhost:5432/tbde_test')
    op_columns = [
            "symbol",
            "timestamp",
            "ltp",
            "ltq",
            "oi",
            "bid",
            "bid_qty",
            "ask",
            "ask_qty",
            "expiry_date"]

    create_table_func(op_columns, 'company_temp_3', 'public', 'timestamp')
    with Pool(8) as pool:
        #for batch in range(0,len(list_of_files)-1, 20):
        df = pool.map(main, list_of_files)
        #time.sleep(3)

    pool.close()
    pool.join()
    toc = time.perf_counter()
    logger.info(toc - tic)
