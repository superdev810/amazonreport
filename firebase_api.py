from firebase import firebase
import time

# CONFIG
db_url = "https://amazonreport-4a8bb.firebaseio.com/"


# APP SETUP - DO NOT TOUCH
firebase = firebase.FirebaseApplication(db_url, authentication=None)

# Saves an object into Parse.com.
def save_object(class_name, data):
    if "createdAt" not in data:
        data["createdAt"] = time.time()

    result = firebase.post('/' + class_name, data)
    return result

def update_object(class_name, object_id, data):
    result = firebase.put('/' + class_name, name=object_id, data=data)
    return result

def patch_object(url, data):
    result = firebase.patch(url, data=data)
    return result

# Finds all object instances in Parse with given class_name, and given query.
def query_objects(class_name):
    result = firebase.get('/' + class_name, None)
    return result
