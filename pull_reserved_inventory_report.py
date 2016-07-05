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

#report_type = '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_'
#report_type = '_GET_FLAT_FILE_OPEN_LISTINGS_DATA_'
#report_type = '_GET_MERCHANT_LISTINGS_DATA_'
#report_type = '_GET_FLAT_FILE_ORDERS_DATA_'-aa
#report_type = '_GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_'
#report_type = '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_'
#report_type = '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_'
#report_type = '_GET_FLAT_FILE_PENDING_ORDERS_DATA_'-aa
#report_type = '_GET_SELLER_FEEDBACK_DATA_'-aa
#report_type = '_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_'
#report_type = '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_'
#report_type = '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_'
#report_type = '_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA_'
#report_type = '_GET_AFN_INVENTORY_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_MONTHLY_INVENTORY_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_INVENTORY_RECEIPTS_DATA_'
report_type = '_GET_RESERVED_INVENTORY_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_INVENTORY_SUMMARY_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_INVENTORY_ADJUSTMENTS_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_INVENTORY_HEALTH_DATA_'
#report_type = '_GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_CROSS_BORDER_INVENTORY_MOVEMENT_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_INBOUND_NONCOMPLIANCE_DATA_'
#report_type = '_GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA_'
#report_type = '_GET_FBA_REIMBURSEMENTS_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA_'-aa
#report_type = '_GET_FBA_RECOMMENDED_REMOVAL_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA_'
#report_type = '_GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA_'-aa
#report_type = '_GET_FLAT_FILE_SALES_TAX_DATA_'

users = firebase_api.query_objects('users')

for sellerid, user in users.iteritems():
    authToken = user['authToken']
    markets = user['marketplaces']

    for marketname, marketplaceid in markets.iteritems():
        result = mws_reports.run_report_pipeline(sellerid, authToken, report_type, marketplaceid)

        i = 0
        if result != None:
            for data in result:
                if data != None and data['sku'] != None:
                    sku = data['sku'].replace('.', '*');

                    if data['product_name'] != None:
                        data['product_name'] = data['product_name'].decode('latin1')

                    i = i + 1
                    print i
                    print data
                    firebase_api.update_object('reports/' + sellerid + '/' + marketplaceid + '/' + report_type + '/', sku, data)
        else:
            print "No report"
#print marketplaces