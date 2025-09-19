import pandas as pd
import numpy as np
import pyreadstat
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Mapeos específicos por módulo
from config.factors_mapping import FACTORS_MAPPING
from config.modules_config import MODULES_MAPPING

class ENAHOLoader:
    def __init__(self, base_path):
        self.base_path = Path(base_path)

    def limpiar_columnas(self, df, tipo_modulo):
        """
        Limpia nombres de columnas con manejo específico por módulo.
        """
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()

        # Renombrar factores específicos por módulo
        if tipo_modulo in FACTORS_MAPPING:
            for old_name, new_name in FACTORS_MAPPING[tipo_modulo].items():
                if old_name in df.columns:
                    df = df.rename(columns={old_name: new_name})

        # Estandarizar nombres especiales
        rename_dict = {
            'aÑo': 'año',
            'añO': 'año',
            'AÑO': 'año'
        }
        df = df.rename(columns=rename_dict)

        return df


    def cargar_modulo(self, año, tipo_modulo):
        """Carga un módulo específico para un año dado."""
        año_path = self.base_path / str(año) / 'DTA'
        
        if not año_path.exists():
            print(f"Carpeta no encontrada: {año_path}")
            return None
            
        # Construir nombre del archivo
        nombre_archivo = MODULES_MAPPING.get(tipo_modulo, '').format(año=año)
        ruta_archivo = año_path / nombre_archivo
        
        print(f"Intentando cargar: {ruta_archivo}")  # Debug
        
        if ruta_archivo.exists():
            try:
                df = pd.read_stata(str(ruta_archivo), convert_categoricals=False)
                df = self.limpiar_columnas(df, tipo_modulo)
                print(f"Módulo {tipo_modulo} cargado exitosamente")
                return df
            except Exception as e:
                print(f"Error cargando {ruta_archivo}: {e}")
        else:
            print(f"Archivo no encontrado: {ruta_archivo}")
        return None


    def cargar_datos_año(self, año):
        """Carga todos los módulos configurados para un año específico."""
        modulos = {}
        
        for tipo_modulo in MODULES_MAPPING.keys():
            try:
                df = self.cargar_modulo(año, tipo_modulo)
                modulos[tipo_modulo] = df
            except Exception as e:
                print(f"Error cargando módulo {tipo_modulo}: {e}")
                modulos[tipo_modulo] = None
        
        return modulos
