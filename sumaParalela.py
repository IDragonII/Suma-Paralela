import mysql.connector
from multiprocessing import Pool
from tqdm import tqdm
import logging
import json
import time
import matplotlib.pyplot as plt
from simular_error import simular_error

logging.basicConfig(filename='suma_paralela.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def cargar_configuracion():
    with open('config.json', 'r') as f:
        return json.load(f)

def conectar_bd(config):
    return mysql.connector.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )

def obtener_suma_rango(start, end, proceso_id, config, intentos=3):
    tiempo_inicio = time.time()
    while intentos > 0:
        try:
            if simular_error(0.5): #50% de error, desactivacion en simular_error.py
                raise ConnectionError("Error simulado de conexión a la base de datos.")
                
            conn = conectar_bd(config)
            cursor = conn.cursor()
            query = f"SELECT numero FROM numeros WHERE id BETWEEN {start} AND {end}"
            cursor.execute(query)
            numeros = cursor.fetchall()

            if any(not isinstance(numero[0], (int, float)) for numero in numeros):
                raise ValueError("Datos no válidos encontrados en el rango.")

            suma_parcial = sum(numero[0] for numero in numeros)

            cursor.close()
            conn.close()

            tiempo_total = time.time() - tiempo_inicio
            logging.info(f"Proceso {proceso_id}: Suma parcial de IDs {start} a {end} = {suma_parcial} - Correcto. Tiempo: {tiempo_total:.4f} segundos")
            print(f"Proceso {proceso_id}: Suma parcial de IDs {start} a {end} = {suma_parcial} - Correcto. Tiempo: {tiempo_total:.4f} segundos")
            return suma_parcial, tiempo_total
        except Exception as e:
            intentos -= 1
            logging.error(f"Proceso {proceso_id}: Error al procesar IDs {start} a {end} - {str(e)}")
            print(f"Proceso {proceso_id}: Error al procesar IDs {start} a {end}, {intentos} reintentos restantes...")
            if intentos == 0:
                print(f"Proceso {proceso_id}: Fallo definitivo al procesar IDs {start} a {end}")
                return None, None

def suma_paralela(total_hilos, total_datos, config):
    rango = total_datos // total_hilos
    rangos = [(i * rango + 1, (i + 1) * rango, i + 1) for i in range(total_hilos)]
    rangos[-1] = (rangos[-1][0], total_datos, total_hilos)

    resultados = []
    tiempos = []
    errores_en_procesos = 0

    with Pool(total_hilos) as pool:
        with tqdm(total=total_hilos) as pbar:
            for resultado, tiempo in pool.starmap(obtener_suma_rango, [(start, end, proc_id, config) for start, end, proc_id in rangos]):
                if resultado is not None:
                    resultados.append(resultado)
                    tiempos.append(tiempo)
                else:
                    resultados.append(0)
                    errores_en_procesos += 1
                pbar.update(1)

    if errores_en_procesos > 0:
        print(f"\nNo se pudo completar la operación debido a {errores_en_procesos} errores durante el procesamiento.")
    else:
        suma_total = sum(resultados)
        print(f"\nSuma total de los {total_datos} números: {suma_total} (sin errores)")

    plt.figure(figsize=(10, 5))
    plt.bar(range(1, total_hilos + 1), resultados, color='blue', alpha=0.7, label='Sumas Parciales')
    plt.axhline(y=sum(resultados), color='r', linestyle='--', label='Suma Total')
    plt.xlabel('Número de Proceso')
    plt.ylabel('Suma Parcial')
    plt.title('Resultados de Sumas Parciales')
    plt.legend()
    plt.grid()
    plt.show()

    logging.info(f"Suma total de los {total_datos} números: {sum(resultados)} (con {errores_en_procesos} errores)")

if __name__ == '__main__':
    total_hilos = 8
    total_datos = 1000000

    config = cargar_configuracion()

    suma_paralela(total_hilos, total_datos, config)
