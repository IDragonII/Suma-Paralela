import random

activar_simulador = False  #True= Activado, False= Desactivado

def simular_error(probabilidad=0.2):
    if activar_simulador:
        return random.random() < probabilidad
    return False
