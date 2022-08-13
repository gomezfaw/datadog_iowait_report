#!/usr/bin/python3

from time import sleep, time
from csv import DictReader

import dd_query
import json
import sqlite3
import os
import click

IOWAIT_TOLERANCE_PERCENTAGE = 2  # Ajustar el porcentaje minimo para considerar que una instancia tiene iowait alto
JSON_REPORTS_PATH = "iowait_reports"


def get_failed_instances_last_week(application):
    result = dd_query.query_events("event:replace_unhealthy,cause:health_check_failure,environment:production,"
                                   "application:fury-pilot,application:" + application, "1644721200", "1645325940")
    return result


def get_failed_instances_events_range(scope, application, t_start, t_end):
    result = dd_query.query_events("event:replace_unhealthy,cause:health_check_failure,environment:production,"
                                   "application:fury-pilot,application:" + application + ",origin_scope:" + scope,
                                   str(t_start), str(t_end))
    return result


def get_failed_instances_events_last_week(scope, application):
    result = dd_query.query_events("event:replace_unhealthy,cause:health_check_failure,environment:production,"
                                   "application:fury-pilot,application:" + application + ",origin_scope:" + scope,
                                   "1644721200", "1645325940")
    return result


def get_instances_iowait(scope, application, timestamp):
    query1 = 'avg:system.cpu.iowait{application:' + application + ',scope:' + scope + '} by {host}'
    return dd_query.query_scalar(query1, int(timestamp) - 60, int(timestamp) + 60)


def high_iowait_present(instances_info):
    try:
        instances_name = instances_info["data"][0]["attributes"]["columns"][0]["values"]
        instances_iowait = instances_info["data"][0]["attributes"]["columns"][1]["values"]
    except Exception as e:
        print(e)
        print("Error reading IOWait metrics, for:", instances_info, "\ncontinue to next app.")
        return []
    instances_iowait_true = []
    for i in range(len(instances_iowait)):
        # Ajustar sensibilidad para determinar si es o no iowait
        if cast_float(instances_iowait[i]) > IOWAIT_TOLERANCE_PERCENTAGE:
            instances_iowait_true.append(
                {"instance": clean_instance_name(instances_name[i]), "iowait": instances_iowait[i]})
    return instances_iowait_true


def find_iowait_instances(application, events):
    instances_with_io_wait = []
    for event in events:
        query_result = get_instances_iowait(application["scope_name"], application["app_name"], event["date_happened"])
        result = high_iowait_present(query_result)
        if len(result) > 0:
            for i in range(len(result)):
                instances_with_io_wait.append({"event_id": event["id"],
                                               "app_id": application["newrelic_id"],
                                               "scope_name": application["scope_name"],
                                               "app_name": application["app_name"],
                                               "date_happened": event["date_happened"],
                                               "instance": result[i]["instance"],
                                               "iowait": result[i]["iowait"]})
    return instances_with_io_wait


def clean_instance_name(instance_name):
    return str(instance_name).translate({ord(i): None for i in "'[]"})


def cast_float(num):
    try:
        return float(num)
    except (ValueError, TypeError):
        return 0


def save_json(path, filename, json_info):
    os.makedirs(path, exist_ok=True)
    with open(path + "/" + filename + ".json", "w") as out_file:
        json.dump(json_info, out_file, indent=4)


def rpt_failed_iowait_instances(bd_conn, app, t_start=str(int(time()) - 604800), t_end=str(int(time())), output="db"):
    failed_instance_events = get_failed_instances_events_range(app['scope_name'], app['app_name'], t_start, t_end)
    iowait_instances = []
    if len(failed_instance_events['events']) > 0:  # Si es mayor que cero hay eventos del tipo replace_unhealthy
        iowait_instances = find_iowait_instances(app, failed_instance_events['events'])
    print(app['scope_name'] + "," + app['app_name'] + ","
          + str(len(failed_instance_events['events'])) + "," + str(len(iowait_instances)))
    if output == "json":
        data_result = [get_dict_node(app, failed_instance_events, iowait_instances)]
        save_json(JSON_REPORTS_PATH, app["scope_name"] + "." + app["app_name"], data_result)
    else:
        save_report_to_database(bd_conn, app, failed_instance_events, iowait_instances)


def get_dict_node(application, events, iowait_instances):
    new_dict_node = {
        "scope_name": application['scope_name'],
        "app_name": application['app_name'],
        "unhealthy_events_total": len(events['events']),
        "iowait_events_total": len(iowait_instances),
        "unhealthy_events": events['events'],
        "iowait_instances": iowait_instances
    }
    return new_dict_node


def get_list_node(application, events):
    new_list_node = [{
        "scope_name": application['scope_name'],
        "app_name": application['app_name'],
        "type": application['type'],
        "newrelic_id": application['newrelic_id'],
        "project_code": application['project_code'],
        "team_name": application['team_name'],
        "unhealthy_events_total": len(events['events']),
        "unhealthy_events": events['events']
    }]
    return new_list_node


def save_report_to_database(bd_conn, app, instances_fail, iowait_instances):
    insert_into_applications(bd_conn, app, len(instances_fail['events']), len(iowait_instances))
    insert_into_unhealthy_events(bd_conn, app, instances_fail['events'])
    insert_into_iowait_events(bd_conn, iowait_instances)


def insert_into_applications(db_conn, application, total_unhealthy, total_iowait):
    db_conn.execute("insert or ignore into applications values(?,?,?,?,?,?,?,?)",
                    (
                        application.get("newrelic_id", "None"), application["scope_name"], application["app_name"],
                        application["type"], application["project_code"], application["team_name"], total_unhealthy,
                        total_iowait)
                    )
    db_conn.commit()


def insert_into_unhealthy_events(db_conn, application, events):
    for event in events:
        db_conn.execute("insert or ignore into unhealthy_events values(?,?,?,?,?,?)",
                        (event["id"], event["incident_date"], event["alert_type"], event["text"], event["host"],
                         application["newrelic_id"])
                        )
    db_conn.commit()


def insert_into_iowait_events(db_conn, iowait_instances_list):
    for event in iowait_instances_list:
        db_conn.execute("insert or ignore into iowait_events values(?,?,?,?)",
                        (event["instance"], event["iowait"],
                         event["app_id"], event["event_id"])
                        )
    db_conn.commit()


def database_setup(db_conn):
    db_conn.execute('''CREATE TABLE IF NOT EXISTS applications (newrelic_id int primary key, scope_name char, 
    app_name char, type char, project_code char, team_name char, unhealthy_events_total int, iowait_events_total 
    INT)''')
    db_conn.execute('''CREATE TABLE IF NOT EXISTS "iowait_events" (instance_id text, iowait_percentage real, 
    app_id int, event_id int, FOREIGN KEY(app_id) REFERENCES applications(newrelic_id), FOREIGN KEY(event_id) 
    REFERENCES unhealthy_events(id))''')
    db_conn.execute('''CREATE TABLE IF NOT EXISTS unhealthy_events (id int primary key, incident_date text, 
    alert_type text, text_desc text, host text, app_id int, FOREIGN KEY(app_id) REFERENCES applications(
    newrelic_id))''')
    db_conn.commit()


def check_st_time(start_date, end_date):
    st = int(start_date)
    et = int(end_date)
    if et > st:
        return start_date
    else:
        return str(et - 604800)


@click.command()
@click.argument('app_src')
@click.option('--end-date', type=str, default=str(int(time())))
@click.option('--start-date', type=str, default=str(int(time())-604800))
@click.option('--output', type=str, default="db")
def main(app_src, start_date, end_date, output):
    start_date = check_st_time(start_date, end_date)
    db = sqlite3.connect("dd_report_iowait.sqlite")
    database_setup(db)
    print("scope,app_name,number_of_fails,IOWaitEvents")
    if app_src.endswith(".csv"):
        with open(app_src, 'r') as apps_file:
            csv_apps = DictReader(apps_file)
            for app in csv_apps:
                rpt_failed_iowait_instances(db, app, start_date, end_date, output)
                sleep(1.5)
    else:
        app_info = app_src.split(".")
        app = {'scope_name': app_info[0], 'app_name': app_info[1]}
        rpt_failed_iowait_instances(db, app, start_date, end_date, output)
    db.close()


if __name__ == '__main__':
    main()
