# Import functions and objects the microservice needs.
# - Flask is the top-level application. You implement the application by adding methods to it.
# - Response enables creating well-formed HTTP/REST responses.
# - requests enables accessing the elements of an incoming HTTP/REST request.
#
from flask import Flask, Response, request

from datetime import datetime
import json

# Setup and use the simple, common Python logging framework. Send log messages to the console.
# The application should get the log level out of the context. We will change later.
#
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

from src.data_tables.RDBDataTable import RDBDataTable
import generate_links

_data_tables = {}


###################################################################################################################

def say_hello(username = "World"):
    return '<p>Hello %s!</p>\n' % username


# some bits of text for the page.
header_text = '''
    <html>\n<head> <title>W4111 GoT</title> </head>\n<body>'''
instructions = '''
    <p><em>Hint</em>: This is a RESTful web service! Append a username
    to the URL. The routes /health and /demo do interesting things.</p>\n'''
home_link = '<p><a href="/">Back</a></p>\n'
footer_text = '</body>\n</html>'

# EB looks for an 'application' callable by default.
# This is the top-level application that receives and routes requests.
application = Flask(__name__)

##################################################################################################################
_default_context = None

def _get_default_context():

    global _default_context

    return _default_context


_tables = {}

def _get_table(db_name, t_name):

    key = db_name + "." + t_name
    tbl = _tables.get(key, None)

    if tbl is None:
        tbl = RDBDataTable(key)
        _tables[key] = tbl

    return tbl



# 1. Extract the input information from the requests object.
# 2. Log the information
# 3. Return extracted information.
#
def log_and_extract_input(method, path_params=None):

    path = request.path
    args = dict(request.args)
    data = None
    headers = dict(request.headers)
    method = request.method

    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        data = "You sent something but I could not get JSON out of it."

    log_message = str(datetime.now()) + ": Method " + method

    field_list = args.get('fields', None)
    if field_list is not None:
        field_list = field_list.split(",")
        del (args['fieldlist'])

    inputs =  {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data,
        "field_list": field_list
        }

    log_message += " received: \n" + json.dumps(inputs, indent=2)
    logger.debug(log_message)

    return inputs

def log_response(method, status, data, txt):

    msg = {
        "method": method,
        "status": status,
        "txt": txt,
        "data": data
    }

    logger.debug(str(datetime.now()) + ": \n" + json.dumps(msg, indent=2, default=str))




# This function performs a basic health check. We will flesh this out.
@application.route("/", methods=["GET"])
def index_rsp():

    rsp_txt = header_text + say_hello() + instructions + footer_text
    rsp = Response(rsp_txt, status=200, content_type="text/html")
    return rsp


# This function performs a basic health check. We will flesh this out.
@application.route("/health", methods=["GET"])
def health_check():

    rsp_data = { "status": "healthy", "time": str(datetime.now()) }
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp


@application.route("/api/demo/<parameter>", methods=["GET", "POST"])
@application.route("/api/demo/", methods=["GET", "POST"])
def demo(parameter=None):

    inputs = log_and_extract_input(demo, { "parameter": parameter })

    msg = {
        "/demo received the following inputs" : inputs
    }

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp

@application.route("/api/<dbname>/<tablename>", methods=["GET"])
def basic_table(dbname, tablename):


    inputs = log_and_extract_input(demo, { "parameters": { "dbname": dbname, "tablename": tablename} })
    rsp_data = None
    rsp_status = None
    rsp_txt = None

    try:

        tbl = _get_table(dbname, tablename)

        if inputs["method"] == "GET":

            rsp = tbl.find_by_template(template=inputs['query_params'], field_list=inputs['field_list'])

            if rsp is not None:
                rsp_data = rsp
                rsp_status = 200
                rsp_txt = "OK"
            else:
                rsp_data = None
                rsp_status = 404
                rsp_txt = "NOT FOUND"
        else:
            rsp_data = None
            rsp_status = 501
            rsp_txt = "NOT IMPLEMENTED"

        if rsp_data is not None:
            rsp_data = generate_links.add_links(dbname, tablename, rsp_data)
            full_rsp = Response(json.dumps(rsp_data, default=str), status=rsp_status, content_type="application/json")
        else:
            full_rsp = Response(rsp_txt, status=rsp_status, content_type="text/plain")

    except Exception as e:
        log_msg = "/dbname/tablename: Exception = " + str(e)
        logger.error(log_msg)
        rsp_status = 500
        rsp_txt = "INTERNAL SERVER ERROR. Please take COMSE6156 -- Cloud Native Applications."
        full_rsp = Response(rsp_txt, status=rsp_status, content_type="text/plain")

    log_response("/dbname/tablename", rsp_status, rsp_data, rsp_txt)

    return full_rsp

"""
@application.route("/api/user/<email>", methods=["GET", "PUT", "DELETE"])
def user_email(email):

    global _user_service

    inputs = log_and_extract_input(demo, { "parameters": email })
    rsp_data = None
    rsp_status = None
    rsp_txt = None

    try:

        user_service = _get_user_service()

        logger.error("/email: _user_service = " + str(user_service))

        if inputs["method"] == "GET":

            rsp = user_service.get_by_email(email)

            if rsp is not None:
                rsp_data = rsp
                rsp_status = 200
                rsp_txt = "OK"
            else:
                rsp_data = None
                rsp_status = 404
                rsp_txt = "NOT FOUND"
        else:
            rsp_data = None
            rsp_status = 501
            rsp_txt = "NOT IMPLEMENTED"

        if rsp_data is not None:
            full_rsp = Response(json.dumps(rsp_data), status=rsp_status, content_type="application/json")
        else:
            full_rsp = Response(rsp_txt, status=rsp_status, content_type="text/plain")

    except Exception as e:
        log_msg = "/email: Exception = " + str(e)
        logger.error(log_msg)
        rsp_status = 500
        rsp_txt = "INTERNAL SERVER ERROR. Please take COMSE6156 -- Cloud Native Applications."
        full_rsp = Response(rsp_txt, status=rsp_status, content_type="text/plain")

    log_response("/email", rsp_status, rsp_data, rsp_txt)

    return full_rsp
"""

logger.debug("__name__ = " + str(__name__))
# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.

    logger.debug("Starting Project GoT at time: " + str(datetime.now()))


    application.debug = True
    application.run()