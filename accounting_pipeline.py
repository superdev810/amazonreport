import firebase_api
import mws_reports
from mws import mws
import xml.etree.ElementTree as ET
import time

ACCESS_KEY = 'AKIAJZ6R7P2H65YKTHLQ' #replace with your access key
SECRET_KEY = 'BDmbHcnfrWCB0i33bMN2YqW13UHG2ptV3s5HYqzs' #replace with your secret key

usersDict = firebase_api.query_objects("users")

reports = ["_GET_MERCHANT_LISTINGS_DATA_", "_GET_AFN_INVENTORY_DATA_", "_GET_FBA_REIMBURSEMENTS_DATA_", "_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_"]

progress_data = {}

pipeline_finished = False

while not pipeline_finished:
	for objectId in usersDict:
		found_unfinished_user = False
		user = usersDict[objectId]
		username = user["username"]

		if "sellerId" not in user:
			#print("Skipping " + username)
			continue
		seller_id = user["sellerId"]
		auth_token = user["authToken"]
		if username not in progress_data:
			found_unfinished_user = True
			progress_data[username] = {}
			for report_type in reports:
				try:
					data = mws_reports.request_report(seller_id, auth_token, report_type)
					progress_data[username][report_type] = {}
					progress_data[username][report_type]["status"] = data["reportProcessingStatus"]
					progress_data[username][report_type]["requestId"] = data["reportRequestId"]
				except Exception as e:
					print("Failed requesting report for " + username, e)

		else:
			print("Checking updates for " + username + ":")
			for report_type in reports:
				if report_type not in progress_data[username]:
					continue
				else:
					print(username, report_type, progress_data[username][report_type]["status"])

				report_status = progress_data[username][report_type]["status"]
				if report_status != "_DONE_":
					found_unfinished_user = True

					reports_api = mws.Reports(access_key=ACCESS_KEY, secret_key=SECRET_KEY, account_id=seller_id, auth_token=auth_token)

					request_id = progress_data[username][report_type]["requestId"]
					feed_response_obj = reports_api.get_report_request_list(requestids=[request_id])

					xml = feed_response_obj.response.content
					#print(xml)
					root = ET.fromstring(xml)

					getReportRequestListResult = root[0]
					hasNext = getReportRequestListResult[0]
					reportRequestInfo = getReportRequestListResult[1]
					reportType = reportRequestInfo[0]
					reportProcessingStatus = reportRequestInfo[1]

					progress_data[username][report_type]["status"] = reportProcessingStatus.text

					if reportProcessingStatus.text == "_DONE_":
						endDate = reportRequestInfo[2]
						scheduled = reportRequestInfo[3]
						reportRequestId = reportRequestInfo[4]
						startedProcessingDate = reportRequestInfo[5]
						submittedDate = reportRequestInfo[6]
						startDate = reportRequestInfo[7]
						completedDate = reportRequestInfo[8]
						generatedReportId = reportRequestInfo[9]
						progress_data[username][report_type]["status"] = reportProcessingStatus.text
						progress_data[username][report_type]["reportId"] = generatedReportId.text
					elif reportProcessingStatus.text == "_CANCELLED_":
						# don't consider this an unfinished user, so we have this elif branch
						# to prevent execution of the else statement
						pass
					else:
						found_unfinished_user = True
			if not found_unfinished_user:
				firebase_api.patch_object("reports/" + username + "/", progress_data[username])
				print("All reports for " + username + " have been successfully processed.")
			print("\n")

	print("FINISHED ITERATION.\n\n\n\n\n")
	time.sleep(60)

	if not found_unfinished_user:
		#firebase_api.save_object("reports", progress_data)
		pipeline_finished = True


