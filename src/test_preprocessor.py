"""
Script de prueba para el preprocesador de datos.
"""

import sys 
import os
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data_loader import ENAHOLoader
from src.preprocessor import ENAHOPreprocessor

def test_preprocessor():
    """
    Prueba completa del preprocesador de datos.
    """

    print("Iniciando prueba del preprocesador...")
    print("="*50)
    
    try:
        # Inicializar componentes
        loader = ENAHOLoader("./data/1. raw")
        preprocessor = ENAHOPreprocessor()

        # Probar con el año más reciente disponible
        test_year = 2024 

        # 1. Cargar módulos
        print(f"Cargando módulos para el año {test_year}...")
        datos_crudos = loader.cargar_datos_año(test_year)
        if not datos_crudos:
            print("No se cargaron datos. Verifica los archivos y rutas.")
            return False
        print(f"Módulos cargados: {list(datos_crudos.keys())}")

        # 2. Preprocesar cada módulo
        print("Preprocesando módulos...")
        modulos_procesados = {}
        for modulo, df in datos_crudos.items():
            if df is not None:
                df_procesado = preprocessor.preprocesar_datos(df, modulo)
                modulos_procesados[modulo] = df_procesado
                print(f" {modulo}:{df.shape} -> {df_procesado.shape if df_procesado is not None else 'ERROR'}")

        # 3. Empalmar módulos
        print("Empalmando módulos...")
        datos_empalmados = preprocessor.empalmar_modulos_año(modulos_procesados)
        print(f"Datos empalmados: {datos_empalmados.shape}")

        # 4. Validación factores de expansión
        print(f"Factores de expansión encontrados:")
        factor_columns = [col for col in datos_empalmados.columns if 'factor' in col]
        for factor in factor_columns:
            print(f" - {factor}")

        # 5. Verificar columnas clave
        print("\nVerificando columnas clave (incluyendo sufijos)...")
        key_columns_to_check = ['conglome', 'vivienda', 'hogar', 'codperso']
        suffixes = ['', '_sum', '_viv', '_per', '_edu', '_emp', '_x', '_y']
    
        all_keys_found = True
        for col in key_columns_to_check:
            found = False
            for suffix in suffixes:
                full_col_name = f"{col}{suffix}" if suffix else col
                if full_col_name in datos_empalmados.columns:
                    print(f"   - {full_col_name}: check")
                    found = True
                    break
            if not found:
                print(f"   - {col}: (no encontrada con ningún sufijo)")
                all_keys_found = False

        # 6. Mostrar primeras columnas para debugging
        print("\nPrimeras 20 columnas del dataset:")
        for i, col in enumerate(datos_empalmados.columns[:20]):
            print(f"   {i+1:2d}. {col}")

        if not all_keys_found:
            print("\n Advertencia: Algunas columnas clave no se encontraron")
            return False

        print("\nFactores de expansión requeridos:")
        required_factors = ['factor07_sum', 'factor07', 'factora07']
        for factor in required_factors:
            if factor in datos_empalmados.columns:
                print(f"{factor}")
            else:
                print(f"{factor} (no encontrado)")

        # 7. Prueba de indicadores (SOLO SI las columnas clave están presentes)
        print("\nProbando sistema de indicadores...")
        from src.indicators import IndicatorCalculator

        calculator = IndicatorCalculator(datos_empalmados)

        # Listar indicadores disponibles
        print(f"Indicadores disponibles: {calculator.list_indicators()}")

        # Calcular algunos indicadores de ejemplo
        try:
            tamano_hogar = calculator.calculate('tamano_hogar')
            print(f"Tamaño hogar: {tamano_hogar.shape if tamano_hogar is not None else 'N/A'}")
            
            jefatura = calculator.calculate('jefatura_hogar')
            print(f"Jefatura hogar: {jefatura.shape if jefatura is not None else 'N/A'}")
            
        except Exception as e:
            print(f"Error en indicadores: {e}")
            return False

        return True

    except Exception as e:
        print(f"Error durante la prueba del preprocesador: {e}")
        return False

if __name__ == "__main__":
    exito = test_preprocessor()
    if exito:
        print("Prueba del preprocesador completada con éxito.")
    else:
        print("La prueba del preprocesador falló.")