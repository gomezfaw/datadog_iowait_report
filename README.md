# datadog_iowait_report
Python script to generate a report of application scopes that have failed instances whit high io-wait in certain period of time.

## Requerimientos
Los requerimientos están en requirements.txt: requests, requests-cache, click

## Modo de uso
```
dd_report_iowait.py [OPTIONS] [APP_NAME | APP_LIST.CSV]

APP_NAME: Full aplication scope name ej: prod-mlb.shipping-logistics-shipments
APP_LIST.CSV: File containing a list of scope,app_name in the same path of the script, a template is provided in the repo

Options:
  --end-date: Timestamp (10 digits) of the final point in time for the report, default current time.
  --start-date: Timestamp (10 digits) of the start point in time for the report, default a week ago.
  --output: Report output format, default is 'db' it creates a sqlite db, also json is supported, it generates a /report/ forder with individual json file for each scope.application.
```