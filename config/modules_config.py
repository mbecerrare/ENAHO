MODULES_MAPPING = {
    'vivienda': 'enaho01-{año}-100.dta',
    'personas': 'enaho01-{año}-200.dta',
    'educacion': 'enaho01a-{año}-300.dta',
    'empleo_ingresos': 'enaho01a-{año}-500.dta',
    'sumarias': 'sumaria-{año}.dta'
}

# Patrones de nombres de archivo para cada módulo
MODULE_PATTERNS = {
    'vivienda': 'enaho01-{year}-100.dta',
    'personas': 'enaho01-{year}-200.dta',
    'educacion': 'enaho01a-{year}-300.dta',
    'empleo_ingresos': 'enaho01a-{year}-500.dta',
    'sumarias': 'sumaria-{year}.dta'
}

# Columnas clave para merge/join entre módulos
KEY_COLUMNS = {
    'vivienda': ['conglome', 'vivienda', 'hogar'],
    'personas': ['conglome', 'vivienda', 'hogar', 'codperso'],
    'educacion': ['conglome', 'vivienda', 'hogar', 'codperso'],
    'empleo_ingresos': ['conglome', 'vivienda', 'hogar', 'codperso'],
    'sumarias': ['conglome', 'vivienda', 'hogar']
}

# Variables críticas para validación y análisis
CRITICAL_VARS = {
    'sumarias': ['factor07', 'factor', 'mieperho', 'pobreza', 'dominio', 'estrato'],
    'vivienda': ['p101', 'p102', 'p103', 'p104', 'p105'],
    'personas': ['p203', 'p204', 'p205', 'p207'],
    'educacion': ['p306', 'p307'],
    'empleo_ingresos': ['ocu500']
}