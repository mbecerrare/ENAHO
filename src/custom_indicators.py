"""
Plantilla para indicadores personalizados - ENAHO
"""

import pandas as pd
import numpy as np

def mi_indicador_personalizado(df, factor_col='factor07_sumaria', **kwargs):
    """
    Ejemplo de indicador personalizado.
    
    Args:
        df: DataFrame con datos ENAHO empalmados
        factor_col: Columna de factor de expansión a usar
        **kwargs: Argumentos adicionales
        
    Returns:
        DataFrame con resultados del indicador
    """
    # Implementar lógica del indicador aquí
    resultado = df.groupby(['año', 'dominio']).agg({
        factor_col: 'sum'
    }).reset_index()
    
    return resultado

# Diccionario de indicadores personalizados
CUSTOM_INDICATORS = {
    'mi_indicador': mi_indicador_personalizado
}

# Ejemplo de uso:
# calculator.register_indicator('nuevo_indicador', calcular_nuevo_indicador)