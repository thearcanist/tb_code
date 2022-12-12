import os
import csv
from time import perf_counter
from datetime import datetime


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
            #logger.info(num_yr)
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



def add_csv_columns(read_path, write_path):
    # os.chdir(read_path)
    i=0

    for file in os.listdir(read_path):
        try:

            #i+=1

            #if i%10000 == 0:
                #just to see the progress
            #    print(i)

            # serial_number = (file[:8])
            split_file_name = split_file_names(file)
            symbol = split_file_name[0]
            expiry_date = split_file_name[1]
            creader = csv.reader(open(read_path + '/' + file))
            cwriter = csv.writer(open(write_path + '/' + file, 'w+'))

            for cline in creader:
                new_line = [val for col, val in enumerate(cline) if col != 1]
                new_line[0] = str(datetime.strptime(new_line[0], '%Y%m%d').date()) + ' ' + cline[1]
                new_line.insert(0, symbol)
                new_line.insert(10, expiry_date)
                #cwriter.writerow(new_line)

        except Exception as e:
            print(e)
            print('problem with file: ' + file)
            pass



if __name__ == "__main__":
    tic = perf_counter()
    read_path = '/home/tbde/truedata-historical-data/JAN-22-Testing'
    write_path = '/home/tbde/truedata-historical-data/JAN-22-Testing-write'
    add_csv_columns(read_path, write_path)
    toc = perf_counter()
    print(toc - tic)
