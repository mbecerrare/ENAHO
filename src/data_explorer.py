import pandas as pd
import os
from pathlib import Path
from config.modules_config import MODULES_MAPPING

def explorar_datos(base_path):
    """
    Explora la estructura de datos y verifica la disponibilidad de archivos.
    """

    años = range(2004, 2024)
    modulos_encontrados = {}

    for año in años:
        año_path = Path(base_path) / str(año) / 'DTA'
        if not año_path.exists():
            print(f"Año {año}: Directorio no encontrado.")
            continue

        archivos = list(año_path.glob('*.dta'))
        modulos_encontrados[año] = []

        for archivo in archivos:
            tipo_modulo = identificar_modulo(archivo.name)
            tamaño_archivo = archivo.stat().st_size / (1024 * 1024)  # Tamaño en MB
            modulos_encontrados[año].append({
                'archivo': archivo.name,
                'tipo_modulo': tipo_modulo,
                'tamaño_MB': tamaño_archivo
            })

            if len(modulos_encontrados[año] <= 3):
                try:
                    df = pd.read_stata(archivo, nrows=5)
                    print(f"\n {archivo.name}: {len(df.columns)} columnas")
                    print(f" Columnas: {df.columns.tolist()}")
                except:
                    print(f" No se pudo leer {archivo.name}")
    return modulos_encontrados

def identificar_modulo(nombre_archivo):
    """
    Identifica el tipo de módulo basado en el nombre del archivo.
    """
    if 'sumaria' in nombre_archivo:
        return 'sumarias'
    elif 'enaho01' in nombre_archivo:
        #Extraer el número después del año

        partes = nombre_archivo.split('_')
        if len(partes) >= 3:
            codigo_modulo = partes[2].split('.')[0]
            return MODULES_MAPPING.get(codigo_modulo, 'desconocido')
        return 'desconocido'

#Ejecutar la exploración
if __name__ == "__main__":
    base_path = "../data/raw"
    estructura_datos = explorar_datos(base_path)
    print(estructura_datos)

    for año, modulos in estructura_datos.items():
        print(f"\nAño {año}:")
        for modulo in modulos:
            print(f" - {modulo['archivo']}: {modulo['tipo_modulo']} ({modulo['tamaño_MB']:.2f} MB)")
 
