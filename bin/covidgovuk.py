import sys, logging
import requests
from pprint import pprint
import json
from lxml import etree
from helpers import load_checkpoint, save_checkpoint, print_xml_stream, get_input_config, get_validation_config, do_validate


#set up logging
logging.root
logging.root.setLevel(logging.WARNING)
formatter = logging.Formatter('%(levelname)s %(message)s')
#with zero args , should go to STD ERR
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.root.addHandler(handler)


SCHEME = """<scheme>
    <title>COVID-19 UK Only - Gov.Uk Stats</title>
    <description>Poll for Covid-19 UK Regional case stats</description>
    <use_external_validation>true</use_external_validation>
    <streaming_mode>xml</streaming_mode>
    <use_single_instance>false</use_single_instance>

    <endpoint>
        <args>
            <arg name="name">
                <title>Covid input name</title>
                <description>Name of this input</description>
            </arg>
            <arg name="http_proxy">
                <title>HTTP Proxy Address</title>
                <description>HTTP Proxy Address</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>false</required_on_create>
            </arg>
            <arg name="https_proxy">
                <title>HTTPs Proxy Address</title>
                <description>HTTPs Proxy Address</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>false</required_on_create>
            </arg>
            <arg name="request_timeout">
                <title>Request Timeout</title>
                <description>Timeout when making HTTP requests</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>false</required_on_create>
            </arg>
        </args>
    </endpoint>
</scheme>
"""

def iterate_json_data(block, data_json):
    for region, region_data in data_json.items():
        if region != 'metadata':
            splunk_row={}
            splunk_row['type'] = block
            splunk_row['region'] = region
            if 'lastUpdatedAt' in data_json:
                splunk_row['date'] = data_json['lastUpdatedAt']
            elif 'metadata' in data_json and 'lastUpdatedAt' in data_json['metadata']:
                splunk_row['date'] = data_json['metadata']['lastUpdatedAt']

#            print ("region={}".format(region))
            if isinstance(region_data, int):
                splunk_row['value'] = region_data
                print_xml_stream(json.dumps(splunk_row,ensure_ascii=False),block)
            else:
                for detail, detail_data in region_data.items():
    #                print("detail={}".format(detail))
                    splunk_row['detail'] = detail
                    if not isinstance(detail_data, list):
    #                    print("value={}".format(detail_data['value']))
                        splunk_row[detail] = detail_data['value']
                        print_xml_stream(json.dumps(splunk_row,ensure_ascii=False),block)
                        del splunk_row[detail]
                    else:
                        for dailydate in detail_data:
                            #print("DailyDate={}".format(dailydate))
                            splunk_row[detail] = dailydate['value']
                            if 'date' in dailydate:
                                splunk_row['date'] = dailydate['date'] + " 01:00:00.000"

                            pprint(splunk_row)
                            print_xml_stream(json.dumps(splunk_row,ensure_ascii=False),block)
                            del splunk_row[detail]

def do_run():

    config = get_input_config()

    #setup some globals
    global STANZA
    STANZA = config.get("name")

    http_proxy = config.get("http_proxy")
    https_proxy = config.get("https_proxy")

    proxies = {}

    if not http_proxy is None:
        proxies["http"] = http_proxy
    if not https_proxy is None:
        proxies["https"] = https_proxy


    request_timeout = int(config.get("request_timeout", 30))

    try:
        req_args = {"verify" : True , "timeout" : float(request_timeout)}
        if proxies:
            req_args["proxies"] = proxies

        #Population data
        data_url = "https://c19pub.azureedge.net/assets/population/population.json"
        print("Processing file={}".format(data_url))
        data_req = requests.get(url=data_url, params=req_args)
        data_json = data_req.json()
        iterate_json_data("population", data_json)

        # Test data
        json_files = ["utlas", "overview", "regions", "countries"]
        for json_file in json_files:
            data_url = "https://c19downloads.azureedge.net/downloads/data/{}_latest.json".format(json_file)
            data_req = requests.get(url=data_url, params=req_args)
            data_json = data_req.json()
            iterate_json_data(json_file, data_json)

    except RuntimeError, e:
        logging.error("Looks like an error: %s" % str(e))
        sys.exit(2)

def usage():
    print "usage: %s [--scheme|--validate-arguments]"
    logging.error("Incorrect Program Usage")
    sys.exit(2)

def do_scheme():
    print SCHEME

if __name__ == '__main__':

    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":
            do_scheme()
        elif sys.argv[1] == "--validate-arguments":
            do_validate()
        else:
            usage()
    else:
        do_run()

    sys.exit(0)

