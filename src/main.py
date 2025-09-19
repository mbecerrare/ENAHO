"""
Pipeline principal ENAHO - Procesamiento completo de datos
"""
import sys
import os
import time
from pathlib import Path
from datetime import datetime

# Agregar el directorio raíz al path
project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

# Importaciones después de configurar el path
from src.data_loader import ENAHOLoader
from src.preprocessor import ENAHOPreprocessor
from src.indicators import IndicatorCalculator
from src.storage import StorageManager

class ENAHOPipeline:
    def __init__(self):
        # Usar Path para manejar rutas
        base_path = Path("D:/Mateo/ICSI/ENAHO")
        self.loader = ENAHOLoader(base_path / "data" / "1. raw")
        self.preprocessor = ENAHOPreprocessor()
        self.storage = StorageManager(base_path / "data")
    
    def procesar_año(self, año, calcular_indicadores=True):
        """
        Procesa completamente un año de datos ENAHO.
        """
        print(f"\n{'='*60}")
        print(f"PROCESANDO AÑO {año}")
        print(f"{'='*60}")
        
        start_time = time.time()
        
        try:
            # 1. Cargar datos
            print("Cargando módulos...")
            datos_crudos = self.loader.cargar_datos_año(año)
            if not datos_crudos:
                print(f"⏭Saltando año {año} - datos incompletos")
                return False
            
            # 2. Preprocesar
            print("Preprocesando...")
            modulos_procesados = {}
            for modulo, df in datos_crudos.items():
                if df is not None:
                    df_procesado = self.preprocessor.preprocesar_datos(df, modulo)
                    modulos_procesados[modulo] = df_procesado
                    print(f" {modulo}:{df.shape} -> {df_procesado.shape}")
            
            # 3. Empalmar
            print("Empalmando módulos...")
            datos_empalmados = self.preprocessor.empalmar_modulos_año(modulos_procesados)
            if datos_empalmados is None:
                raise ValueError("Error al empalmar módulos")
            print(f"   Datos empalmados: {datos_empalmados.shape}")
            
            # 4. Guardar datos empalmados
            print("Guardando datos empalmados...")
            merged_path = self.storage.save_merged_data(datos_empalmados, año)
            print(f"   Guardado en: {merged_path}")
            
            # 5. Calcular y guardar indicadores
            if calcular_indicadores:
                print("Calculando indicadores...")
                calculator = IndicatorCalculator(datos_empalmados)
                indicadores = calculator.calculate_all()
                
                print("Guardando indicadores...")
                indicator_paths = self.storage.save_indicators(indicadores, año)
                print(f"   Indicadores guardados en: {self.storage.processed_path / 'Indicators'}")
            
            elapsed = time.time() - start_time
            print(f"✓ {año} completado en {elapsed:.2f} segundos")
            return True
            
        except Exception as e:
            print(f"✗ Error procesando {año}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def procesar_rango_años(self, años, calcular_indicadores=True):
        """
        Procesa un rango de años.
        """
        resultados = {}
        
        for año in años:
            éxito = self.procesar_año(año, calcular_indicadores)
            resultados[año] = éxito
        
        # Resumen final
        print(f"\n{'='*60}")
        print("RESUMEN FINAL")
        print(f"{'='*60}")
        
        exitosos = sum(resultados.values())
        total = len(resultados)
        
        print(f"Años procesados exitosamente: {exitosos}/{total}")
        print(f"Porcentaje de éxito: {exitosos/total*100:.1f}%")
        
        if exitosos < total:
            print("\nAños con errores:")
            for año, éxito in resultados.items():
                if not éxito:
                    print(f"  - {año}")
        
        return resultados

def main():
    """
    Función principal del pipeline.
    """
    print("INICIANDO PIPELINE ENAHO 2004-2024")
    print("=" * 60)
    
    # Configurar años a procesar
    años = list(range(2004, 2025))
    
    pipeline = ENAHOPipeline()
    
    # Procesar años
    resultados = pipeline.procesar_rango_años(años)
    
    # Mostrar resumen de almacenamiento
    print(f"\nArchivos generados:")
    print(f"  Datos unidos: {pipeline.storage.processed_path / 'Merged'}")
    print(f"  Indicadores: {pipeline.storage.processed_path / 'Indicators'}")
    
    print(f"\nPipeline completado a las {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()