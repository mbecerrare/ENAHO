"""
Módulo para manejo de almacenamiento de datos ENAHO
"""
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

class StorageManager:
    def __init__(self, base_path):
        """
        Inicializa el gestor de almacenamiento.
        
        Args:
            base_path (str|Path): Ruta base donde están las carpetas de datos
        """
        self.base_path = Path(base_path)
        self.processed_path = self.base_path / "2. processed"
        self.final_path = self.base_path / "3. final"
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Crea las carpetas necesarias si no existen."""
        for path in [self.processed_path / "Merged", 
                    self.processed_path / "Indicators",
                    self.final_path]:
            path.mkdir(parents=True, exist_ok=True)
    
    def save_merged_data(self, df, año):
        """
        Guarda datos unidos en formato parquet con partición por año.
        
        Args:
            df (DataFrame): DataFrame con datos unidos
            año (int): Año de los datos
            
        Returns:
            Path: Ruta donde se guardaron los datos
        """
        output_path = self.processed_path / "Merged" / f"enaho_{año}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path
    
    def save_indicators(self, indicators_dict, año):
        """
        Guarda indicadores calculados en formato CSV.
        
        Args:
            indicators_dict (dict): Diccionario de DataFrames con indicadores
            año (int): Año de los datos
            
        Returns:
            dict: Diccionario con rutas de archivos guardados
        """
        results = {}
        valid_indicators = {
            name: df for name, df in indicators_dict.items() 
            if df is not None
        }
        
        if not valid_indicators:
            print("No hay indicadores válidos para guardar")
            return results
        
        for name, df in valid_indicators.items():
            output_path = self.processed_path / "Indicators" / f"{name}_{año}.csv"
            df.to_csv(output_path, index=False)
            results[name] = str(output_path)
        
        # Guardar metadata solo de indicadores válidos
        metadata = {
            "año": año,
            "fecha_proceso": datetime.now().isoformat(),
            "indicadores": list(valid_indicators.keys()),
            "filas_por_indicador": {name: len(df) for name, df in valid_indicators.items()}
        }
        
        meta_path = self.processed_path / "Indicators" / f"metadata_{año}.json"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        return results
    
    def load_merged_data(self, año):
        """
        Carga datos unidos de un año específico.
        
        Args:
            año (int): Año a cargar
            
        Returns:
            DataFrame|None: DataFrame con datos o None si no existe
        """
        file_path = self.processed_path / "Merged" / f"enaho_{año}.parquet"
        if file_path.exists():
            return pd.read_parquet(file_path)
        return None
    
    def list_processed_years(self):
        """
        Lista los años que tienen datos procesados.
        
        Returns:
            list: Lista de años encontrados
        """
        pattern = "enaho_*.parquet"
        files = list(self.processed_path.glob(pattern))
        años = [int(f.stem.split('_')[1]) for f in files]
        return sorted(años)
