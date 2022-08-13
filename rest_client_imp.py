from time import sleep

import requests
import requests_cache

OK_RESPONSES = [200, 201, 202]
MAX_RETRIES = 5
RATE_LIMIT_RETRIES = 15
RATE_LIMIT_RETRY_TIME_WAIT = 300
RETRY_TIME_WAIT = 10
error_counter = 0
error_rate_counter = 0

CACHE_TTL = 21600


requests_cache.install_cache(cache_name='rest_client_cache', backend='sqlite', expire_after=CACHE_TTL)


def make_rest_call(url="", headers="", body="", method="get", retries=MAX_RETRIES):
    global error_counter
    global error_rate_counter
    try:
        if method == "get":
            response = requests.get(url, headers=headers)
        elif method == "post":
            response = requests.post(url, headers=headers, data=body)
    except requests.exceptions.RequestException as e:
        error_counter += 1
        if error_counter > retries:
            print(e)
            print(f"Error final, se supero el maximo ({retries}) de retries configurado")
            raise SystemExit(e)
        print(f"Error en la conexion, esperando {RETRY_TIME_WAIT}s para reintentar...")
        sleep(RETRY_TIME_WAIT)
        return make_rest_call(url=url, headers=headers, body=body, retries=retries)
    if response.status_code == 429:  # Hemos recibido un rate limit, debemos parar un tiempo largo
        error_rate_counter += 1
        print("Datadog status error:", response.status_code)
        print("Cabeceras:", response.headers)
        if error_rate_counter > RATE_LIMIT_RETRIES:
            print(f"Error final, se supero el maximo ({RATE_LIMIT_RETRIES}) de retries por rate limit configurado")
            return None
        print(f"Se recibio error de rate limit, esperando {RATE_LIMIT_RETRY_TIME_WAIT}s para reintentar...")
        sleep(RATE_LIMIT_RETRY_TIME_WAIT)
        return make_rest_call(url=url, headers=headers, body=body, retries=retries)
    if response.status_code not in OK_RESPONSES:
        error_counter += 1
        if error_counter > retries:
            print(f"Error final, se supero el maximo ({retries}) de retries configurado")
            return None
        print("Received error code:", response.status_code)
        print("Headers:", response.headers)
        print("url:", response.url)
        print(f"Waiting {RETRY_TIME_WAIT}s to retry...")
        sleep(RETRY_TIME_WAIT)
        return make_rest_call(url=url, headers=headers, body=body, retries=retries)
    error_counter = error_rate_counter = 0  # Restablecer los contadores de error porque la peticion fue exitosa.
    return response.json()  # Llegados a este punto es que el query se ejecuto bien, recibimos un codigo 2XX retornamos


if __name__ == '__main__':
    print(make_rest_call(url="https://reqres.in/api2/products/3", method="get"))  # Url de testeo del consumo
