import os
from flask import Flask, send_file, render_template
import pymongo
from app import app
from datetime import date
import csv
import re
'''
Đầu vào: Dữ liệu các mã cổ phiếu.
Đầu ra: Các file excell được download trực tiếp từ website ứng với từng mã"
'''
myclient = pymongo.MongoClient("mongodb+srv://ducthangbnn:Oivung1215@cluster0.1rpru.mongodb.net/test", connect=False)
mydb = myclient["stocks"]
mycol = mydb["auto_stock_info_old"]


@app.route('/', methods=['GET', 'POST'])
def table():

    stocks_infors = mycol.find().sort("date", -1)
    return render_template('index.html', cache_timeout=0,
                          stocks_infors = stocks_infors)

