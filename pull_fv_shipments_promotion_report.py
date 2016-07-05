import firebase_api
import mws_reports
import xml.etree.ElementTree as ET
import time
import inspect
import datetime
import json
import os
import csv

report_type = '_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA_'

users = firebase_api.query_objects('users')

for sellerid, user in users.iteritems():
    authToken = user['authToken']
    markets = user['marketplaces']

    for marketname, marketplaceid in markets.iteritems():
        result = mws_reports.run_report_pipeline(sellerid, authToken, report_type, marketplaceid)

        i = 0
        if result != None:
            for data in result:
                if data != None:
                    order_id = data['amazon_order_id']

                    if order_id != None:
                        print data
                        firebase_api.update_object('reports/' + sellerid + '/' + marketplaceid + '/' + report_type + '/', order_id, data)
        else:
            print "No report"

