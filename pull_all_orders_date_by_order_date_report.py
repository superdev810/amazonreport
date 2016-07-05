import firebase_api
import mws_reports
import xml.etree.ElementTree as ET
import time
import inspect
import datetime
import json
import os
import csv

report_type = '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_'

users = firebase_api.query_objects('users')

for sellerid, user in users.iteritems():
    authToken = user['authToken']
    markets = user['marketplaces']

    for marketname, marketplaceid in markets.iteritems():
        result = mws_reports.run_report_pipeline(sellerid, authToken, report_type, marketplaceid)

        if result != None:
            for data in result:
                if data != None and data['sku'] != None:
                    sku = data['sku'].replace('.', '*');
                    orderid = data['amazon_order_id']

                    if data['product_name'] != None:
                        data['product_name'] = data['product_name'].decode('latin1')

                    if data['ship_state'] != None:
                        data['ship_state'] = data['ship_state'].decode('latin1')

                    print data
                    firebase_api.update_object('reports/' + sellerid + '/' + marketplaceid + '/' + report_type + '/' + sku + '/', orderid, data)
        else:
            print "No report"
