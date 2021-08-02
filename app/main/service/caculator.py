import csv
import pandas as pd
import datetime
from selenium import webdriver
from config import configs
import time
import psycopg2
from ftplib import FTP
from datetime import datetime
import pytz
import schedule
import pymongo
from pandas import ExcelWriter
from pandas import ExcelFile

# Đọc dữ liệu về user để logging vào phần mềm.
df = pd.read_excel('user_trading.xlsx', sheet_name=0)
config = configs()

def path_string():
    y = datetime.today().year
    m = datetime.today().month
    d = datetime.today().day
    if (datetime.now().isoweekday() in range(2,6):
        date_ = str(y) + str(m) + str(d-1)  # datetime.today().strftime("%Y%m%d")
        #now = datetime.today().strftime('%Y%m%d') code cu xu ly ngay hien tai
    elif (datetime.now().isoweekday() == 1):    # Tinh huong roi vao thu 2
        date_ = str(y) + str(m) + str(d - 3)  # datetime.today().strftime("%Y%m%d")
    return config["info_fpt"]["fpt_file_name"] + date_ + "_" + date_ + ".csv"

def stock_data(p_str_path):
    p_str_path=p_str_path+path_string()
    df = pd.read_csv(p_str_path)
    return df

def log(p_result):
    f = open("log.txt", "a", encoding="utf-8")
    f.write(p_result+"\n")
    f.close()
def main():
    # Tam bo qua khong dung ftp
    #utility.ftp_file(config["info_fpt"]["ftp_ip"], config["info_fpt"]["ftp_user"], config["info_fpt"]["ftp_password"])
    myclient = pymongo.MongoClient("mongodb+srv://ducthangbnn:Oivung1215@cluster0.1rpru.mongodb.net/test",
                                   connect=False)
    f_result = stock_data(config["info_fpt"]["ftp_file_path"])
    mydb = myclient["stocks"]
    mycol = mydb["auto_stock_info"]

    for i in df.index:
        driver = webdriver.Chrome()
        name = df["ssi_name"][i]
        username = str(df['ssi_username'][i])
        password = str(df['ssi_password'][i])
        account = str(df['ssi_account'][i])
        pin_code = str(df['ssi_pin_code'][i])
        print(df["id"][i])
        log("Name: {0}".format(name))
        log("Starting")
        # Lấy tổng số tiền hiện có
        current_money = mycol.find_one({'id': int(df["id"][i])})["current_money"]
        initial_money = mycol.find_one({'id': int(df["id"][i])})["initial_money"]
        auto_login(driver, username, password, account)
        buy = []
        sell = []
        email_content = ""
        for j in range(len(f_result)):
            symbol = f_result['symbol'][j]
            change = int(f_result['change'][j])*100
            price = float(f_result['price'][j])*1000
            id = int(df["id"][i])
            print(id)
            if change != 0:
                print(symbol, change)
                if change > 0:
                    buy.append(auto_trade(driver, symbol, change, pin_code, current_money))
                    current_money = current_money - (1+0.0025)*(change*price)
                    if current_money < 0:
                        continue
                    else:
                        query = {"$and": [{"id": id}, {"stocks_info.{0}".format(symbol): {"$exists": True}}]}
                        cursor = mycol.find_one(query)
                        if cursor is None:
                            print("cursor is None")
                            mycol.update({'id':  id}, {'$set': {'stocks_info.{0}'.format(symbol): {"volume": change,
                                "average_price": price, "total_price": price* change, "real_price": price,
                                "real_total_price": price * change, 'interest_loss': 0}}})
                            mycol.update({'id':  id}, {'$set': {'current_money': current_money}})
                        else:
                            print("cursor is not None")
                            old_volume = mycol.find_one({'id': id})["stocks_info"][symbol]['volume']
                            new_volume = old_volume + change
                            old_average_price = mycol.find_one({'id': id})["stocks_info"][symbol]['average_price']
                            new_average_price = (old_volume * old_average_price + change*price)/(new_volume)
                            new_total_price = new_volume* new_average_price
                            new_real_price = price
                            new_real_total_price = new_volume*new_real_price
                            old_interset_loss = mycol.find_one({'id': id})["stocks_info"][symbol]['interest_loss']
                            new_interset_loss = old_interset_loss + new_real_total_price - new_total_price



                            mycol.update({'id': id}, {'$set': {'stocks_info.{0}'.format(symbol): {"volume": new_volume,
                                                                               "average_price": new_average_price,
                                                                               "total_price": new_total_price,
                                                                               "real_price": new_real_price,
                                                                               "real_total_price": new_real_total_price,
                                                                               'interest_loss': new_interset_loss}}})


                            mycol.update({'id': id}, {'$set': {'current_money': current_money}})

                if change < 0:

                    sell.append(auto_trade(driver, symbol, change, pin_code, current_money))
                    change = abs(change)
                    query = {"stocks_info.{0}".format(symbol): {"$exists": True}}
                    cursor = mycol.find_one(query)
                    current_money = current_money + (1-0.0025)*change * price
                    if cursor is None:
                        continue
                    else:
                        old_volume = mycol.find_one({'id': id})["stocks_info"][symbol]['volume']
                        new_volume = old_volume - change
                        if new_volume <=0:
                            change = old_volume
                            old_average_price = mycol.find_one({'id': id})["stocks_info"][symbol]['average_price']
                            new_real_price = price
                            old_interset_loss = mycol.find_one({'id': id})["stocks_info"][symbol]['interest_loss']
                            new_interset_loss = old_interset_loss + change*price-old_volume*old_average_price
                            current_money = current_money + change*price
                            mycol.update({'id': id}, {'$unset': {'stocks_info.{0}'.format(symbol): 1}})
                        else:
                            old_average_price = mycol.find_one({'id': id})["stocks_info"][symbol]['average_price']
                            new_average_price = old_average_price
                            new_total_price = new_volume * new_average_price
                            new_real_price = price
                            new_real_total_price = new_volume * new_real_price
                            old_interset_loss = mycol.find_one({'id': id})["stocks_info"][symbol]['interest_loss']
                            new_interset_loss = old_interset_loss + new_real_total_price - new_total_price



                            mycol.update({'id': id}, {'$set': {'stocks_info.{0}'.format(symbol): {"volume": new_volume,
                                                                                                           "average_price": new_average_price,
                                                                                                           "total_price": new_total_price,
                                                                                                           "real_price": new_real_price,
                                                                                                           "real_total_price": new_real_total_price,
                                                                                                           'interest_loss': new_interset_loss}}})
                            mycol.update({'id': id}, {'$set': {'current_money': current_money}})

        email_arrays = buy + sell
        for k in range(len(email_arrays)):
            email_content = email_content + str(k+1) + ". " + email_arrays[k] + "\n"

        print(email_content)
        print(str(df['ssi_email'][i]))
        utility.send_email(str(df['ssi_email'][i]), email_content)
        driver.close()
        log("Done")
    f = open("log.txt", "a", encoding="utf-8")
    f.write("======================================================================\n")
    f.close()
