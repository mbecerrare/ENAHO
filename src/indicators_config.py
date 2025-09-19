"""
Configuración de indicadores - Factores de expansión por tipo de análisis
"""

# Mapeo de factores de expansión recomendados por tipo de análisis
FACTOR_RECOMMENDATIONS = {
    'hogar': ['factor07_sum', 'factor07_viv'],
    'persona': ['factor07_per', 'factora07_per'],
    'educacion': ['factor07_edu', 'factora07_edu'],
    'empleo': ['factor07_emp', 'factora07_emp']
}

# Niveles de agregación por defecto
DEFAULT_AGGREGATION_LEVELS = {
    'hogar': ['año', 'dominio', 'estrato'],
    'persona': ['año', 'dominio', 'estrato', 'p207']  
}