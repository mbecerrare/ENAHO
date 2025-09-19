"""
Indicadores base predefinidos para ENAHO
"""

import pandas as pd
import numpy as np

def calcular_tamano_hogar(df, factor_col='factor07_sum'):
    """
    Calcula el tamaño promedio del hogar.
    """
    # Identify available factor column
    available_factors = [col for col in df.columns if 'factor07' in col]
    if available_factors:
        factor_col = available_factors[0]
    
    if 'mieperho' not in df.columns:
        return None
    
    resultado = df.groupby(['conglome', 'vivienda', 'hogar']).agg({
        'mieperho': 'first',
        factor_col: 'first'
    }).reset_index()
    
    return resultado

def calcular_jefatura_hogar(df, factor_col='factor07_sum'):
    """
    Calcula porcentaje de jefatura de hogar por sexo.
    Asume que p203 = 1 es jefe de hogar y p207 es sexo.
    """
    if 'p203' not in df.columns or 'p207' not in df.columns:
        return None
    
    # Filtrar jefes de hogar
    jefes = df[df['p203'] == 1].copy()
    
    # Agrupar y calcular
    resultado = jefes.groupby(['año', 'dominio', 'p207']).agg({
        factor_col: 'sum'
    }).reset_index()
    
    totales = df.groupby(['año', 'dominio']).agg({
        factor_col: 'sum'
    }).reset_index()
    
    # Calcular porcentajes
    resultado = resultado.merge(totales, on=['año', 'dominio'], suffixes=('_jefes', '_total'))
    resultado['porcentaje_jefatura'] = resultado[f'{factor_col}_jefes'] / resultado[f'{factor_col}_total'] * 100
    
    return resultado[['año', 'dominio', 'p207', 'porcentaje_jefatura']]

def calcular_anios_educacion(df, factor_col='factor07_per'):
    """
    Calcula años promedio de educación.
    Asume que p301a contiene los años de educación.
    """
    if 'p301a' not in df.columns:
        return None
    
    # Filtrar valores válidos
    educacion_df = df[df['p301a'].between(0, 20)].copy()
    
    resultado = educacion_df.groupby(['año', 'dominio', 'p207']).agg({
        'p301a': 'mean',
        factor_col: 'sum'
    }).reset_index()
    
    return resultado.rename(columns={'p301a': 'anios_educacion_promedio'})

def calcular_tasa_empleo(df, factor_col='factor07_emp'):
    """
    Calcula tasa de empleo.
    Asume que ocu500 indica condición de ocupación.
    """
    if 'ocu500' not in df.columns:
        return None
    
    # Crear variable binaria de empleo
    df['empleado'] = df['ocu500'].isin([1, 2, 3]).astype(int)
    
    resultado = df.groupby(['año', 'dominio', 'p207']).agg({
        'empleado': 'mean',
        factor_col: 'sum'
    }).reset_index()
    
    return resultado.rename(columns={'empleado': 'tasa_empleo'})

# Diccionario de indicadores base
BASE_INDICATORS = {
    'tamano_hogar': calcular_tamano_hogar,
    'jefatura_hogar': calcular_jefatura_hogar,
    'anios_educacion': calcular_anios_educacion,
    'tasa_empleo': calcular_tasa_empleo
}