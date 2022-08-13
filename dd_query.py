import os
import json
import rest_client_imp

DD_GET_EVENT = "https://api.datadoghq.com/api/v1/events/{event_id}"
DD_GET_EVENTS = "https://api.datadoghq.com/api/v1/events?{query}"
DD_QUERY_METRICS_SCALAR = "https://api.datadoghq.com/api/v2/query/scalar"

'''
DD_OK_RESPONSES = [200, 201, 202]
DD_ERROR_RETRIES = 5
DD_RATE_RETRIES = 15
DD_RATE_TIME_WAIT = 120
DD_ERROR_TIME_WAIT = 60
dd_error_counter = 0
dd_rate_counter = 0
'''


def print_json(json_data):
    print(json.dumps(json_data, sort_keys=True, indent=4))


def get_dd_api_key():
    if os.environ.get('DD_API_KEY') is not None:
        return os.environ.get('DD_API_KEY')
    else:
        print("You must set the env: DD_API_KEY with a valid Datadog API Key")
        exit(1)


def get_dd_app_key():
    if os.environ.get('DD_APP_KEY') is not None:
        return os.environ.get('DD_APP_KEY')
    else:
        print("You must set the env: DD_APP_KEY with a valid Datadog APP Key")
        exit(1)


def get_dd_headers():
    dd_api_key = get_dd_api_key()
    dd_app_key = get_dd_app_key()
    req_headers = {
        'DD-API-KEY': dd_api_key,
        'DD-APPLICATION-KEY': dd_app_key
    }
    return req_headers


def dd_query_json(api_url):
    response = rest_client_imp.make_rest_call(url=api_url, headers=get_dd_headers(), method="get")
    return response


def dd_query_json_post(api_url, body):
    response = rest_client_imp.make_rest_call(url=api_url, headers=get_dd_headers(), body=body, method="post")
    return response


def to_millis(timestamp):
    str_time = str(timestamp)
    while len(str_time) < 13:
        str_time = str_time + "0"
    return str_time


def query_all_events(time_start, time_end):
    query_string = "start={tstart}&end={tend}".format(tstart=time_start, tend=time_end)
    api_url = DD_GET_EVENTS.format(query=query_string)
    return dd_query_json(api_url)


def query_events(query, time_start, time_end):
    query_string = "start={tstart}&end={tend}".format(tstart=time_start, tend=time_end)
    query_string = query_string + "&tags={query}".format(query=query)
    api_url = DD_GET_EVENTS.format(query=query_string)
    return dd_query_json(api_url)


def query_scalar(query, time_start, time_end):
    body = '''{"meta":{"dd_extra_usage_params":{}},
        "data":[{"type":"scalar_request","attributes":{"formulas":[{"formula":"query1"}],
        "queries":[{"query":"''' + query + '''","data_source":"metrics","name":"query1","aggregator":"avg"}],
        "from":''' + to_millis(time_start) + ''',"to": ''' + to_millis(time_end) + '''
        }}]}'''
    return dd_query_json_post(DD_QUERY_METRICS_SCALAR, body)


def query_scalar_memory(query, time_start, time_end):
    query_orig = "application:shipping-logistics-shipments,scope:prod-others"
    body = '''{
        "meta":{
             "dd_extra_usage_params":{ },
             "use_multi_step":true
             },
        "data":[{
            "type":"scalar_request",
            "attributes":{
                "formulas":[{
                    "formula":"query1 - query2",
                    "limit":{
                     "count":25,
                     "order":"desc"
                }
                }],
                "queries":[
                    {
                        "query":"max:system.mem.used{''' + query + '''} by {host}.fill(null)",
                        "data_source":"metrics",
                        "name":"query1",
                        "aggregator":"avg"
                    },
                    {
                        "query":"max:system.mem.cached{''' + query + '''} by {host}.fill(null)",
                        "data_source":"metrics",
                        "name":"query2",
                        "aggregator":"avg"
               }],
            "from":''' + to_millis(time_start) + ''',
            "to":''' + to_millis(time_end) + '''
         }
      }]
    }'''
    return dd_query_json_post(DD_QUERY_METRICS_SCALAR, body)


def query_scalar_cpu(query, time_start, time_end):
    body = '''{
        "meta":{
             "dd_extra_usage_params":{ },
             "use_multi_step":true
             },
        "data":[{
            "type":"scalar_request",
            "attributes":{
                "formulas":[{
                    "formula": "100 - query1",
                    "limit": {
                        "count": 25,
                        "order": "desc"
                    }
                }],
                "queries":[
                    {
                        "query":"avg:system.cpu.idle{''' + query + '''} by {host}.fill(null)",
                        "data_source":"metrics",
                        "name":"query1",
                        "aggregator":"max"
                    }
                    ],
            "from":''' + to_millis(time_start) + ''',
            "to":''' + to_millis(time_end) + '''
         }
      }]
    }'''
    body = '''
    {"meta":{"dd_extra_usage_params":{
    "widget_id":"8563772536997250",
    "is_user_initiated":true,
    "initiated_by":"uninstrumented_user_action",
    "is_visible":true},"use_multi_step":true},
    "data":[{
        "type":"scalar_request",
        "attributes":{
            "formulas":[
                {"formula":"100 - queryavg","limit":{"count":25,"order":"desc"}},
                {"formula":"100 - querymin"},
                {"formula":"100 - querymax"}
                ],
            "queries":[
                {"query":"avg:system.cpu.idle{''' + query + '''}.fill(null)",
                 "data_source":"metrics","name":"queryavg","aggregator":"avg"},
                {"query":"max:system.cpu.idle{''' + query + '''}.fill(null)",
                 "data_source":"metrics","name":"querymin","aggregator":"max"},
                {"query":"min:system.cpu.idle{''' + query + '''}.fill(null)",
                 "data_source":"metrics","name":"querymax","aggregator":"min"}
                 ],
            "from":''' + to_millis(time_start) + ''',
            "to":''' + to_millis(time_end) + '''
            }
        }],
    "_authentication_token":"98a89b4167989708066cb67593d9f01b5a946739"}
    '''
    return dd_query_json_post(DD_QUERY_METRICS_SCALAR, body)


if __name__ == '__main__':
    query1 = 'avg:system.cpu.iowait{application:fbm-inbound,scope:read-stock} by {host}'
    result = query_scalar(query1, 1646772500, 1646772600)
    print_json(result)
