import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime
import io
import time


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
            print(num_yr)
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


def create_df(list_of_files, columns, base_loc=''):
    complete_df = pd.DataFrame()
    for file in list_of_files:
        file_path = base_loc + '/' + file
        split_file_name = split_file_names(file)
        df = pd.read_csv(file_path, header=None)
        df.columns = columns
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time
        df['timestamp'] = pd.to_datetime(df['date'].astype('str') + ' ' + df['time'].astype('str'))
        df['symbol'] = split_file_name[0]
        df['expiry_date'] = split_file_name[1]
        complete_df = pd.concat([complete_df,df])
    complete_df = complete_df.drop(['date','time'], axis=1)
    return complete_df


def push_to_psql(df, table_name, engine, schema='public'):
    # drops old table and creates new empty table
    df.head(0).to_sql(table_name, con=engine, schema=schema, if_exists='replace',index=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="") # null values become ''
    conn.commit()


def main():
    tic = time.perf_counter()
    base_loc = '/home/tbde/truedata-historical-data/JAN-22-Testing'
    list_of_files = os.listdir(base_loc)

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

    complete_df = create_df(list_of_files, columns, base_loc=base_loc)

    #engine = create_engine('postgresql://newuser@localhost:5432/psql_test')

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

    #complete_df = complete_df[op_columns]

    #push_to_psql(complete_df, 'company_temp_2', engine, schema='public')
    toc = time.perf_counter()
    print(toc-tic)


if __name__ == "__main__":
    main()

