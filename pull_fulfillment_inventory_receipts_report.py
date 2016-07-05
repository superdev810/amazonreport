import firebase_api
import mws_reports
import xml.etree.ElementTree as ET
import time
import inspect
import datetime
import json
#import reports_parser
import os
import csv

report_type = '_GET_FBA_FULFILLMENT_INVENTORY_RECEIPTS_DATA_'

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

                    if data['product_name'] != None:
                        data['product_name'] = data['product_name'].decode('latin1')

                    print data
        else:
            print "No report"
