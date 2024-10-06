import mysql.connector
import random

conexion = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="paralel_suma"
)

cursor = conexion.cursor()

lote = []
for _ in range(1000000):
    numero = f"({random.randint(1, 100)})"
    lote.append(numero)
    
    if len(lote) == 10000:
        query = f"INSERT INTO numeros (numero) VALUES {','.join(lote)}"
        cursor.execute(query)
        conexion.commit()
        lote = []

if lote:
    query = f"INSERT INTO numeros (numero) VALUES {','.join(lote)}"
    cursor.execute(query)
    conexion.commit()

cursor.close()
conexion.close()

print("Inserción completada.")
