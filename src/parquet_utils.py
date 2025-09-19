"""
Utilidades para manejo eficiente de archivos Parquet
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path

def optimizar_para_parquet(df):
    """
    Optimiza un DataFrame para almacenamiento en Parquet.
    """
    df_optimizado = df.copy()
    
    # Convertir columnas de texto a categorías cuando sea eficiente
    for col in df_optimizado.select_dtypes(include=['object']).columns:
        if df_optimizado[col].nunique() / len(df_optimizado) < 0.5:  # Menos del 50% valores únicos
            df_optimizado[col] = df_optimizado[col].astype('category')
    
    # Optimizar tipos numéricos
    for col in df_optimizado.select_dtypes(include=['int64']).columns:
        if df_optimizado[col].min() >= 0:
            if df_optimizado[col].max() < 255:
                df_optimizado[col] = df_optimizado[col].astype('uint8')
            elif df_optimizado[col].max() < 65535:
                df_optimizado[col] = df_optimizado[col].astype('uint16')
            elif df_optimizado[col].max() < 4294967295:
                df_optimizado[col] = df_optimizado[col].astype('uint32')
    
    return df_optimizado

def guardar_parquet(df, filepath, partition_cols=None):
    """
    Guarda un DataFrame en formato Parquet optimizado.
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    
    df_optimizado = optimizar_para_parquet(df)
    tabla = pa.Table.from_pandas(df_optimizado)
    
    if partition_cols:
        # Particionar por columnas específicas
        pq.write_to_dataset(
            tabla,
            root_path=filepath,
            partition_cols=partition_cols,
            compression='snappy'
        )
    else:
        # Guardar como archivo único
        pq.write_table(
            tabla,
            filepath,
            compression='snappy'
        )
    
    # Calcular estadísticas del archivo
    file_size = Path(filepath).stat().st_size / (1024 * 1024)  # MB
    print(f"💾 Guardado: {filepath} ({file_size:.2f} MB)")

def cargar_parquet(filepath):
    """
    Carga un archivo Parquet optimizado.
    """
    return pq.read_table(filepath).to_pandas()