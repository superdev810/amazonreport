from mws_wrapper import mws
import xml.etree.ElementTree as ET
import time
import inspect
import datetime
#import reports_parser
import os
import csv
# Developer Credentials
ACCESS_KEY = 'AKIAJZ6R7P2H65YKTHLQ' #replace with your access key
SECRET_KEY = 'BDmbHcnfrWCB0i33bMN2YqW13UHG2ptV3s5HYqzs' #replace with your secret key



def request_report(seller_id, auth_token, report_type, marketplaceid, curr_try=1, max_try=3):

    marketplaceids = [marketplaceid]
    reports_api = mws.Reports(access_key=ACCESS_KEY, secret_key=SECRET_KEY, account_id=seller_id, auth_token=auth_token)
    feed_response_obj = reports_api.request_report(report_type=report_type, start_date=(datetime.datetime.now()-datetime.timedelta(days=30)).isoformat(), end_date=datetime.datetime.now().isoformat(), marketplaceids=marketplaceids)
    #feed_response_obj = reports_api.request_report(report_type=report_type, start_date=None, end_date=None, marketplaceids=[marketplace_id])

    xml = feed_response_obj.response.content
    root = ET.fromstring(xml)

    requestReportResult = root[0]
    reportRequestInfo = requestReportResult[0]
    reportType = reportRequestInfo[0]
    reportProcessingStatus = reportRequestInfo[1]
    endDate = reportRequestInfo[2]
    scheduled = reportRequestInfo[3]
    reportRequestId = reportRequestInfo[4]
    submittedDate = reportRequestInfo[5]
    startDate = reportRequestInfo[6]

    data = {
        "reportType": reportType.text,
        "reportProcessingStatus": reportProcessingStatus.text,
        "endDate": endDate.text,
        "scheduled": scheduled.text,
        "reportRequestId": reportRequestId.text,
        "submittedDate": submittedDate.text,
        "startDate": startDate.text,
    }
    return data


def get_report_request_list(seller_id, auth_token, request_ids, curr_try=1, max_try=3):
    try: 
        reports_api = mws.Reports(access_key=ACCESS_KEY, secret_key=SECRET_KEY, account_id=seller_id, auth_token=auth_token)
        feed_response_obj = reports_api.get_report_request_list(requestids=request_ids)

        xml = feed_response_obj.response.content
        #print(xml)
        root = ET.fromstring(xml)

        getReportRequestListResult = root[0]
        hasNext = getReportRequestListResult[0]
        reportRequestInfo = getReportRequestListResult[1]
        reportType = reportRequestInfo[0]
        reportProcessingStatus = reportRequestInfo[1]
        if reportProcessingStatus.text != "_DONE_":
            print(request_ids)
            print("STATUS: " + reportProcessingStatus.text)
            if curr_try >= max_try:
                return None

            time.sleep(60)
            return get_report_request_list(seller_id, auth_token, request_ids, curr_try=curr_try+1, max_try=3)

        print("and we got here!!!")
        print(xml)
        endDate = reportRequestInfo[2]
        scheduled = reportRequestInfo[3]
        reportRequestId = reportRequestInfo[4]
        startedProcessingDate = reportRequestInfo[5]
        submittedDate = reportRequestInfo[6]
        startDate = reportRequestInfo[7]
        completedDate = reportRequestInfo[8]
        generatedReportId = reportRequestInfo[9]

        data = {
            "reportType": reportType.text,
            "reportProcessingStatus": reportProcessingStatus.text,
            "endDate": endDate.text,
            "scheduled": scheduled.text,
            "reportRequestId": reportRequestId.text,
            "startedProcessingDate": startedProcessingDate.text,
            "submittedDate": submittedDate.text,
            "startDate": startDate.text,
            "completedDate": completedDate.text,
            "generatedReportId": generatedReportId.text
        }
        return data


    except Exception as e:
        print(e)

        if curr_try >= max_try:
            return None
        else:
            time.sleep(60)
            return get_report_request_list(seller_id, auth_token, request_ids, curr_try+1, max_try)


def get_report(seller_id, auth_token, report_id, curr_try=1, max_try=3):
    try: 
        reports_api = mws.Reports(access_key=ACCESS_KEY, secret_key=SECRET_KEY, account_id=seller_id, auth_token=auth_token)
        print("at get report, report id: " + report_id)
        feed_response_obj = reports_api.get_report(report_id=report_id)
        tsv = feed_response_obj.response.content
        return tsv 
    except Exception as e:
        print(e)
        if curr_try >= max_try:
            return None
        else:
            time.sleep(60)
            return get_report(seller_id, auth_token, report_id, curr_try+1, max_try)

def get_report_list(seller_id, auth_token, curr_try=1, max_try=3):
    try: 
        reports_api = mws.Reports(access_key=ACCESS_KEY, secret_key=SECRET_KEY, account_id=seller_id, auth_token=auth_token)
        feed_response_obj = reports_api.get_report_list(max_count="100", types=[])

        tsv = feed_response_obj.response.content
        return tsv 
    except Exception as e:
        print(e)
        if curr_try >= max_try:
            return None
        else:
            time.sleep(60)
            return get_report_list(seller_id, auth_token, curr_try+1, max_try)


# Listing Reports
def request_active_listings_report(seller_id, auth_token, marketplaceid, curr_try=1, max_try=3):
    report_type = "_GET_MERCHANT_LISTINGS_DATA_"
    return request_report(seller_id, auth_token, report_type, marketplaceid)




def request_open_listings_report(seller_id, auth_token, marketplaceid, curr_try=1, max_try=3):
    report_type = "_GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT_"
    return request_report(seller_id, auth_token, report_type, marketplaceid)

def request_orders_report(seller_id, auth_token, marketplaceid, curr_try=1, max_try=3):
    report_type = "_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_"
    return request_report(seller_id, auth_token, report_type, marketplaceid)

def request_inventory_report(seller_id, auth_token, curr_try=1, max_try=3):
    report_type = "_GET_FLAT_FILE_OPEN_LISTINGS_DATA_"
    return request_report(seller_id, auth_token, report_type)

def request_sold_listings_report(seller_id, auth_token, curr_try=1, max_try=3):
    report_type = "_GET_CONVERGED_FLAT_FILE_SOLD_LISTINGS_DATA_"
    return request_report(seller_id, auth_token, report_type)









def get_orders():
    # Gets all orders
    data = request_orders_report(seller_id, auth_token)
    data = get_report_request_list(seller_id, auth_token, [data["reportRequestId"]])
    print(data["generatedReportId"])
    print get_report(seller_id, auth_token, data["generatedReportId"])


def get_active_listings():
    data = request_active_listings_report(seller_id, auth_token)
    #print(data)
    data = get_report_request_list(seller_id, auth_token, [data["reportRequestId"]])
    #print(data)
    print get_report(seller_id, auth_token, data["generatedReportId"])


def get_sold_listings():
    data = request_sold_listings_report(seller_id, auth_token)
    #print(data)
    data = get_report_request_list(seller_id, auth_token, [data["reportRequestId"]])
    #print(data)
    print get_report(seller_id, auth_token, data["generatedReportId"])


def run_report_pipeline(seller_id, auth_token, report_type, marketplaceids):
    data = request_report(seller_id, auth_token, report_type, marketplaceids)
    print data

    if data == None:
        return None
    data = get_report_request_list(seller_id, auth_token, [data["reportRequestId"]])

    if data == None:
        return None

    report_id = data["generatedReportId"]
    report = get_report(seller_id, auth_token, report_id)
    fname = "reports/" + seller_id + "/" + report_id + ".tsv"
    if not os.path.exists("reports/" + seller_id + "/"):
        os.mkdir("reports/" + seller_id + "/")

    f = open(fname, "w+")
    headers_str = report.split('\n')[0]
    headers_str = headers_str.replace('-', '_')
    report = '\n'.join([headers_str] + report.split('\n')[1:])
    f.write(report)

    rows = []
    with open(fname) as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        for row in reader:
            rows.append(row)

    return rows

