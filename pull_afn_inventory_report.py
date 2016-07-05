import firebase_api
import mws_reports
import xml.etree.ElementTree as ET
import time
import inspect
import datetime
import json
import os
import csv

report_type = '_GET_AFN_INVENTORY_DATA_'

users = firebase_api.query_objects('users')

for sellerid, user in users.iteritems():
    authToken = user['authToken']
    markets = user['marketplaces']

    for marketname, marketplaceid in markets.iteritems():
        result = mws_reports.run_report_pipeline(sellerid, authToken, report_type, marketplaceid)

        if result != None:
            for data in result:
                if data != None and data['seller_sku'] != None:
                    sku = data['seller_sku'].replace('.', '*');

                    print data
                    firebase_api.update_object('reports/' + sellerid + '/' + marketplaceid + '/' + report_type + '/', sku, data)
        else:
            print "No report"
#print marketplaces