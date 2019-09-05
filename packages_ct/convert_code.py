import chardet
import os


def str_judge_code(path):
    # try:
    #     file = open(path, 'rb')
    #     first_line = file.readline()
    # finally:
    #     if file:
    #         file.close()
    with open(path, 'rb') as file:
        first_line = file.readline()

    return chardet.detect(first_line)


def show_code(path):
    return str_judge_code(path)['encoding']


def read_file(path):
    # try:
    #     f = open(path, 'r')
    #     file_content = f.read()
    # finally:
    #     if f:
    #         f.close()

    with open(path, 'r') as f:
        file_content = f.read()

    return file_content


def write_file(content, path):
    try:
        f = open(path, 'w')
        f.write(content)
    finally:
        if f:
            f.close()


# still can't work, !!!!!!!!!!!!!!!!!!
def convert_code(path):
    file_content = read_file(path)
    file_content_byte = file_content.encode('utf_8')
    file_new = file_content_byte.decode('utf-8')
    write_file(file_new, path)


# path = 'E:/1-FuturePlan/Career/lcmf/python/基准和竞品的数据.csv'
# path1 = 'E:/1-FuturePlan/Career/lcmf/python/new.csv'