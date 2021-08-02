import csv
import pandas as pd
import datetime
from selenium import webdriver
import time
import psycopg2
from ftplib import FTP
from datetime import datetime
from datetime import timedelta
import pytz
import schedule
import pymongo
from pandas import ExcelWriter
from pandas import ExcelFile

# Đọc dữ liệu về user để logging vào phần mềm.


myclient = pymongo.MongoClient("mongodb+srv://ducthangbnn:Oivung1215@cluster0.1rpru.mongodb.net/test",
                               connect=False)
mydb = myclient["stocks"]
mycol = mydb["auto_stock_info"]
price_mongo = mydb["price_9h_30"]

def stock_data(p_str_path):
    p_str_path=p_str_path+path_string()
    df = pd.read_csv(p_str_path)
    return df

def log(p_result):
    f = open("log.txt", "a", encoding="utf-8")
    f.write(p_result+"\n")
    f.close()

def find_symbol_previous_day_mongo(date_, symbol ):
    date_previous_one_day = find_previous_date(date_)
    query = {"$and": [{"date": date_previous_one_day}, {"stocks_info.{0}".format(symbol): {"$exists": True}}]}
    cursor = mycol.find_one(query)
    return cursor

def update_ck_kha_dung(date_, symbol):
    date_previous_one_day = find_previous_date(date_)
    date_previous_three_day = find_previous_three_day(date_)
    query = {"$and": [{"date": date_previous_one_day}, {"update_ck_kha_dung.{0}.{1}".format(date_previous_three_day, symbol): {"$exists": True}}]}
    cursor = mycol.find_one(query)
    if cursor is None:
        ck_kha_dung = 0
    else:
        ck_kha_dung = cursor["update_ck_kha_dung"][date_previous_three_day][symbol]
        mycol.update({'date': date_previous_one_day},
                     {'$unset': {'update_ck_kha_dung.{0}.{1}'.format(date_previous_three_day, symbol): 1}})
    return ck_kha_dung

def update_tien_khong_the_mua_symbol(date_, symbol):
    print(date_)
    date_previous_one_day = find_previous_date(date_)
    print(date_previous_one_day)
    date_previous_three_day = find_previous_three_day(date_)
    print(date_previous_three_day)
    query = {"$and": [{"date": date_previous_one_day}, {"update_tien_khong_the_mua.{0}.{1}".format(date_previous_three_day, symbol): {"$exists": True}}]}
    cursor = mycol.find_one(query)
    update_tien_khong_the_mua = 0
    if cursor is None:
        update_tien_khong_the_mua = 0
    else:
        update_tien_khong_the_mua = cursor["update_tien_khong_the_mua"][date_previous_three_day][symbol]
    mycol.update({'date': date_previous_one_day},
                 {'$unset': {'update_tien_khong_the_mua.{0}'.format(date_previous_three_day): 1}})
    return update_tien_khong_the_mua

def update_tien_khong_the_mua(date_):
    money = 0
    print(date_)
    date_previous_one_day = find_previous_date(date_)
    print(date_previous_one_day)
    cursors = mycol.find_one([{"date": date_previous_one_day},{'update_tien_khong_the_mua': {"$exists": True}}])
    if cursors is None:
        money = 0
    else:
        for date__ in cursors.find_one({"date": date_previous_one_day})['update_tien_khong_the_mua']:
            for symbol in cursors[date__]:
                money = money + cursors[date__][symbol]
    return money

def insert_cp_khong_the_ban(date_, symbol, change):
    mycol.update({'date': date_}, {'$set': {'update_ck_kha_dung.{0}.{1}'.format(date_, symbol): int(change)}})

def insert_tien_khong_the_mua(date_, symbol, price_change):
    mycol.update({'date': date_}, {'$set': {'update_tien_khong_the_mua.{0}.{1}'.format(date_, symbol): price_change}})

def get_stock_info_previous_mongo(date_):
    date_previous_one_day = find_previous_date(date_)
    tien_ban_dau =  mycol.find_one({'date': date_previous_one_day})["tien_ban_dau"]
    tien_co_the_mua = mycol.find_one({'date': date_previous_one_day})["tien_co_the_mua"]
    tien_hien_tai = mycol.find_one({'date': date_previous_one_day})["tien_hien_tai"]
    ssi_name = mycol.find_one({'date': date_previous_one_day})["ssi_name"]
    lai_lo_tong = mycol.find_one({'date': date_previous_one_day})["lai_lo_tong"]
    lai_lo_tong_phan_tram = mycol.find_one({'date': date_previous_one_day})["lai_lo_tong_phan_tram"]
    return ssi_name, tien_ban_dau, tien_hien_tai, tien_co_the_mua, lai_lo_tong, lai_lo_tong_phan_tram

def get_stock_info_symbol_previous_mongo(date_, symbol):
    date_previous_one_day = find_previous_date(date_)
    ck_tong = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['ck_tong']
    ck_kha_dung = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['ck_kha_dung']
    gia_tb = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['gia_tb']
    gia_tri = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['gia_tri']
    gia_tt = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['gia_tt']
    gia_tri_tt = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['gia_tri_tt']
    lai_lo = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['lai_lo']
    lai_lo_phan_tram = mycol.find_one({'date': date_previous_one_day})["stocks_info"][symbol]['lai_lo_phan_tram']
    return ck_tong, ck_kha_dung, gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram

def get_stock_gia_tri_tt_symbol_mongo(date_, symbol):
    gia_tri_tt = mycol.find_one({'date': date_})["stocks_info"][symbol]['gia_tri_tt']
    return gia_tri_tt

def get_stock_tien_khong_the_mua_mongo(date_, symbol):
    tien_khong_the_mua = mycol.find_one({'date': date_})["update_tien_khong_the_mua"][date_][symbol]
    return tien_khong_the_mua

def buy_first_time(price, change):
    ck_tong = change
    ck_kha_dung = 0
    gia_tb = price
    gia_tri = gia_tb*ck_tong
    gia_tt = price
    gia_tri_tt = gia_tt*ck_tong
    lai_lo = 0
    lai_lo_phan_tram = 0
    return ck_tong, ck_kha_dung,gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram

def buy_first_time_mongo(date_, symbol, price, change, tien_co_the_mua):
    if tien_co_the_mua < price*change:
        print("Ban khong du tien")
    else:

        ck_tong, ck_kha_dung,gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram = buy_first_time(price, change)
        print(ck_tong, ck_kha_dung,gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram )
        mycol.update({'date': date_}, {'$set': {'stocks_info.{0}'.format(symbol): {"ck_tong": int(ck_tong), "ck_kha_dung": int(ck_kha_dung), "gia_tb": float(gia_tb), "gia_tri": float(gia_tri), "gia_tt": float(gia_tt), "gia_tri_tt": float(gia_tri_tt), 'lai_lo': int(lai_lo), 'lai_lo_phan_tram': float(lai_lo_phan_tram)}}})
        insert_cp_khong_the_ban(date_, symbol, change)
    return gia_tri


def buy_second_time(date_, symbol, price, change):

    date_previous_one_day = find_previous_date(date_)
    ck_tong_previous_one_day, ck_kha_dung_previous_one_day, gia_tb_previous_one_day, gia_tri_previous_one_day, gia_tt_previous_one_day, gia_tri_tt_previous_one_day, lai_lo_previous_one_day, lai_lo_phan_tram_previous_one_day = get_stock_info_symbol_previous_mongo(date_, symbol)

    ck_tong = ck_tong_previous_one_day + change

    ck_kha_dung = ck_kha_dung_previous_one_day + update_ck_kha_dung(date_, symbol)
    gia_tb = (ck_tong_previous_one_day*gia_tb_previous_one_day + change*price)/ck_tong
    gia_tri = ck_tong*gia_tb
    gia_tt = price
    gia_tri_tt = ck_tong*gia_tt
    lai_lo = gia_tri_tt-gia_tri
    lai_lo_phan_tram = lai_lo/gia_tri
    return ck_tong, ck_kha_dung, gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram


def buy_second_time_mongo(date_, symbol, price, change, tien_co_the_mua):
    if tien_co_the_mua < price * change:
        print("Ban khong du tien")
    else:

        ck_tong, ck_kha_dung, gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram = buy_second_time(date_, symbol, price, change)
        mycol.update({'date': date_}, {'$set': {"ck_tong": int(ck_tong), "ck_kha_dung": int(ck_kha_dung), "gia_tb": float(gia_tb), "gia_tri": float(gia_tri), "gia_tt": float(gia_tt), "gia_tri_tt": float(gia_tri_tt), 'lai_lo': int(lai_lo), 'lai_lo_phan_tram': float(lai_lo_phan_tram)}})
        insert_cp_khong_the_ban(date_, symbol, change)


def sell(date_, symbol, price, change):
    change = abs(change)
    date_previous_one_day = find_previous_date(date_)

    cursors = mycol.find_one([{"date": date_previous_one_day}, {'stocks_info.{0}'.format(symbol): {"$exists": True}}])
    if cursors is None:
        print('Làm gì có cổ phiếu mà bán')
        ck_tong = - 1
        ck_kha_dung =0
        gia_tb = 0
        gia_tri = 0
        gia_tt = 0
        gia_tri_tt =0
        lai_lo = 0
        lai_lo_phan_tram = 0
    else:
        ck_tong_previous_one_day, ck_kha_dung_previous_one_day, gia_tb_previous_one_day, gia_tri_previous_one_day, gia_tt_previous_one_day, gia_tri_tt_previous_one_day, lai_lo_previous_one_day, lai_lo_phan_tram_previous_one_day = get_stock_info_symbol_previous_mongo(date_previous_one_day, symbol)
        ck_kha_dung = ck_kha_dung_previous_one_day + update_ck_kha_dung(date_, symbol) - change

        ck_tong = ck_tong_previous_one_day - change

        if ck_kha_dung < 0:
            print("Đã bán quá số cổ phiếu bạn hiện có")
        else:

            gia_tb = gia_tb_previous_one_day
            gia_tri = ck_tong*gia_tb
            gia_tt = price
            gia_tri_tt = ck_tong*gia_tt
            lai_lo = gia_tri_tt-gia_tri
            lai_lo_phan_tram = lai_lo/gia_tri

    return ck_tong, ck_kha_dung, gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram

def sell_mongo(date_, symbol, price, change):
    ck_tong, ck_kha_dung, gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram = sell(date_, symbol,
                                                                                                          price,
                                                                                                          change)
    if ck_tong > 0:
        mycol.update({'date': date_}, {'$set': {'stocks_info.{0}'.format(symbol): {"ck_tong": ck_tong,
                                                                          "ck_kha_dung": ck_kha_dung,
                                                                          "gia_tb": gia_tb,
                                                                          "gia_tri": gia_tri,
                                                                          "gia_tt": gia_tt,
                                                                          "gia_tri_tt": gia_tri_tt,
                                                                          'lai_lo': lai_lo,
                                                                          'lai_lo_phan_tram': lai_lo_phan_tram}}})
        insert_tien_khong_the_mua(date_, symbol, price*abs(change))
    else:
        mycol.update({'date': date_}, {'$unset': {'stocks_info.{0}'.format(symbol): 1}})
        insert_tien_khong_the_mua(date_, symbol, price * abs(change))

def no_sell_or_buy(date_, symbol, price):
    ck_tong_previous_one_day, ck_kha_dung_previous_one_day, gia_tb_previous_one_day, gia_tri_previous_one_day, gia_tt_previous_one_day, gia_tri_tt_previous_one_day, lai_lo_previous_one_day, lai_lo_phan_tram_previous_one_day = get_stock_info_symbol_previous_mongo(date_, symbol)
    ck_kha_dung = ck_kha_dung_previous_one_day + update_ck_kha_dung(date_, symbol)
    ck_tong = ck_tong_previous_one_day
    gia_tb = gia_tb_previous_one_day
    gia_tri = gia_tb_previous_one_day
    gia_tt = price
    gia_tri_tt = ck_tong*gia_tt
    lai_lo = gia_tri_tt-gia_tri
    lai_lo_phan_tram = lai_lo/gia_tri
    return ck_tong, ck_kha_dung, gia_tb, gia_tri, gia_tt, gia_tri_tt, lai_lo, lai_lo_phan_tram

def get_change(date_, symbol):
    date_ = find_previous_date(date_)
    df = pd.read_csv('EMA20_B30_20210701_20210731.csv')
    change = df[(df['date'] == int(date_)) & (df['symbol'] == symbol)]['change'].values
    if len(change) > 0:
        change = change[0]
    else:
        change = 0
    return change

def get_price(date_,symbol):
    date_ = date_[:4]+"-"+date_[4:6]+"-"+date_[6:]
    price = price_mongo.find_one({"$and": [{'date': date_},{'code':symbol}]})['price']
    return price

def get_symbol_csv(date_):
    df = pd.read_csv('EMA20_B30_20210701_20210731.csv')
    date_ = find_previous_date(date_)
    list_symbols = df[(df['date'] == int(date_))]['symbol'].values
    print(list_symbols)
    return list_symbols

def get_symbol_mongo(date_):
    list = []
    date_previous_one_day = find_previous_date(date_)
    cursors = mycol.find({"date": date_previous_one_day})
    for cursor in cursors:
        for symbol in cursor['stocks_info']:
            list.append(symbol)
    return list

def update_mongo(date_):
    insert_date_mongo(date_)
    ssi_name, tien_ban_dau, tien_hien_tai, tien_co_the_mua, lai_lo_tong, lai_lo_tong_phan_tram = get_stock_info_previous_mongo(date_)
    symbols_csv = get_symbol_csv(date_)
    symbols_mongo = get_symbol_mongo(date_)
    # Tiền có thể mua cập nhật
    so_tien_ban = 0
    tien_co_the_mua = tien_co_the_mua + update_tien_khong_the_mua(date_)
    print(tien_co_the_mua)
    for symbol in symbols_csv:
        change = get_change(date_, symbol)*100
        price = get_price(date_, symbol)*1000

        if find_symbol_previous_day_mongo(date_, symbol) is None:
            if change > 0:
                buy_first_time_mongo(date_, symbol, price, change, tien_co_the_mua)
                tien_co_the_mua = tien_co_the_mua - price*change
            if change < 0:
                print("Làm gì có cổ phiếu mà bán")
                no_sell_or_buy(date_, symbol, price)
        else:
            if change > 0:
                buy_second_time_mongo(date_, symbol, price, change, tien_co_the_mua)
                tien_co_the_mua = tien_co_the_mua-price*change
            if change < 0:
                sell(date_, symbol, price, change)
                so_tien_ban = so_tien_ban + abs(change)*price
            if change == 0:
                no_sell_or_buy(date_, symbol, price)

    for symbol in symbols_mongo:
        if symbol not in symbols_csv:
            price = get_price(date_, symbol) * 1000
            no_sell_or_buy(date_, symbol, price)
    gia_tri_tt = 0
    tien_khong_the_mua = 0
    for symbol in symbols_mongo:
        gia_tri_tt = gia_tri_tt + get_stock_gia_tri_tt_symbol_mongo(date_, symbol)
    print(gia_tri_tt)
    for symbol in symbols_mongo:
        tien_khong_the_mua = tien_khong_the_mua + get_stock_tien_khong_the_mua_mongo(date_, symbol)
    print(tien_khong_the_mua)

    tien_hien_tai = gia_tri_tt + tien_khong_the_mua + tien_co_the_mua
    lai_lo_tong = tien_hien_tai - tien_ban_dau
    lai_lo_tong_phan_tram = lai_lo_tong/tien_ban_dau
    mycol.update({'date': date_},
                 {'$set': {'ssi_name': ssi_name, 'tien_ban_dau':tien_ban_dau, 'tien_hien_tai': tien_hien_tai, 'tien_co_the_mua': tien_co_the_mua, 'lai_lo_tong':lai_lo_tong, 'lai_lo_tong_phan_tram':lai_lo_tong_phan_tram}})


date_ = '20210702'
while date_ != '20210707':
    print(date_)
    update_mongo(date_)
    date_ = find_next_date(date_)



