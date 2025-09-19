import pandas as pd
import numpy as np
from config.modules_config import KEY_COLUMNS

class ENAHOPreprocessor:
    def __init__(self):
        self.required_columns = KEY_COLUMNS

    def validar_modulo(self, df, tipo_modulo):
        """Valida que el DataFrame tenga las columnas clave necesarias."""
        required_cols = self.required_columns.get(tipo_modulo, [])
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            print(f"Advertencia: Faltan columnas en el módulo {tipo_modulo}: {missing_cols}")
            return False
        return True
    
    def preprocesar_datos(self, df, tipo_modulo):
        """Realiza preprocesamiento específico según el tipo de módulo."""
        if not self.validar_modulo(df, tipo_modulo):
            return None
        # 1. Maejo valores missing en 
        missing_codes = [999,9999,99999,999999,9999999,99999999]
        for col in df.select_dtypes(include=[np.number]).columns:
            df[col] = df[col].replace(missing_codes, np.nan)
        
        # 2. Convertir tipos de datos si es necesario
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
        # 3. Otras transformaciones específicas del módulo
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            df[col] = df[col].str.strip().str.upper()
        return df
    
    def empalmar_modulos_año(self, modulos_dict):
        """Empalma múltiples módulos de un mismo año en un solo DataFrame."""
        if 'sumarias' not in modulos_dict:
            print("Error: El módulo 'sumarias' es obligatorio para el empalme.")
            return None
        #1. Unir vivienda con sumarias (nivel hogar)
        merged = pd.merge(
            modulos_dict['sumarias'],
            modulos_dict['vivienda'],
            on=['conglome', 'vivienda', 'hogar'],
            how='inner',
            validate='1:1',
            suffixes=('_sum', '_viv')
        )

        # Rename factor columns to match config
        factor_mapping = {
            'factor07': 'factor07_sum',
            'factora07': 'factora07_sum'
        }

        merged = merged.rename(columns=factor_mapping)

        print(f"Merge vivienda-sumarias: {merged.shape}")

        #2. Unir personas (nivel persona)

        if 'personas' in modulos_dict:
            keys = ['conglome', 'vivienda', 'hogar']
            duplicates = modulos_dict['personas'][keys].duplicated()
            if duplicates.any():
                print(f"Encontrados {duplicates.sum()} duplicados en llaves de personas")           

            merged = pd.merge(
                modulos_dict['personas'],
                merged,
                on=keys,
                how='left',
                validate='m:1',
                suffixes=('_per', '')
            )
            print(f"Merge personas: {merged.shape}")

        #3. Unir módulos adicionales (educacion, empleo_ingresos)
        for mod, suffix in [('educacion', '_edu'),('empleo_ingresos', '_emp')]:
            if mod in modulos_dict:
                merged = pd.merge(
                    merged,
                    modulos_dict[mod],
                    on=keys + ['codperso'],
                    how='left',
                    validate='1:1',
                    suffixes=('', suffix)
                )
                print(f"Merge {mod}: {merged.shape}")
        return merged
           







