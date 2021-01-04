# -*- coding: utf-8 -*-
from os import makedirs, walk
from os.path import join
import json

import requests
from pandas import DataFrame


def download_k(stock_code):
    if stock_code[0] == '6':
        tag = 'sh'
    else:
        tag = 'sz'
    full_code = "%s%s" % (tag, stock_code)

    url = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,day,2019-12-31,2020-12-01,300,qfq" % (full_code)
    html = requests.get(url)
    data = json.loads(html.text)

    if 'qfqday' in data['data'][full_code]:
        key = 'qfqday'
    elif 'day' in data['data'][full_code]:
        key = 'day'
    else:
        key = 'day'

    df = DataFrame(data['data'][full_code][key][:][:])
    df = DataFrame(df[[0, 1, 2, 3, 4, 5]])
    df.columns = ['date', 'start', 'end', 'high', 'low', 'volume']

    dir_path = "./csv/%s" % stock_code
    makedirs(dir_path, exist_ok=True)
    file_path = join(dir_path, "%s_%s.csv" % (stock_code, 'kline'))
    df.to_csv(file_path, index=False, encoding="gbk")


def download_financial_table(stock_code, tag):
    url = "http://quotes.money.163.com/service/%s_%s.html" % (tag, stock_code)
    html = requests.get(url)
    html.encoding = "gbk"
    # df = DataFrame(html.text)
    items = html.text.split('\n')
    index = items[0].split(',')[1:-1]
    data = {}
    for item in items:
        t = item.strip().split(',')
        if len(t) == 1:
            continue
        data[t[0]] = t[1:-1]

    date_list = ['2019-12-31', '2018-12-31', '2017-12-31', '2016-12-31', '2015-12-31']
    df = DataFrame(data, index=index)
    df = df[df['报告日期'].isin(date_list)]

    if df.shape[0] < 5:
        return 0

    dir_path = "./csv/%s" % stock_code
    makedirs(dir_path, exist_ok=True)
    file_path = join(dir_path, "%s_%s.csv" % (stock_code, tag))
    df.to_csv(file_path, index=True, encoding="gbk")
    return df.shape[0]


def foreach_stock():
    f = open('./data/stock_list.txt', 'r', encoding='utf-8')
    for line in f:
        if line[0] != '0' and line[0] != '6':
            continue
        else:
            item = line.split('\t')
            yield item[0]
    f.close()


def foreach_dir(target_dir):
    for _, dirs, _ in walk(target_dir):
        for d in dirs:
            yield d


if __name__ == "__main__":
    for code in foreach_stock():
        try:
            if 0 == download_financial_table(code, 'lrb'):
                continue

            download_financial_table(code, 'zcfzb')
            download_k(code)
        except Exception as e:
            print("Error: {0}.Code:{1}".format(e, code))

    for code in foreach_dir('./csv'):
        try:
            download_k(code)
        except Exception as e:
            print("Error: {0}.Code:{1}".format(e, code))
