"""
Sistema principal de cálculo de indicadores ENAHO
"""

import pandas as pd
import numpy as np
from typing import Dict, Callable, Optional
import importlib.util
import sys

class IndicatorCalculator:
    def __init__(self, data: pd.DataFrame):
        self.data = data
        self.indicators = {}
        self._load_base_indicators()
    
    def _load_base_indicators(self):
        """Carga los indicadores base predefinidos"""
        try:
            from src.base_indicators import BASE_INDICATORS
            self.indicators.update(BASE_INDICATORS)
            print("Indicadores base cargados exitosamente")
        except ImportError:
            print("No se pudieron cargar los indicadores base")
    
    def register_indicator(self, name: str, function: Callable):
        """
        Registra un nuevo indicador en el sistema.
        
        Args:
            name: Nombre único del indicador
            function: Función que calcula el indicador
        """
        self.indicators[name] = function
        print(f"Indicador '{name}' registrado exitosamente")
    
    def load_custom_indicators(self, module_path: str):
        """
        Carga indicadores personalizados desde un archivo Python.
        
        Args:
            module_path: Ruta al archivo Python con indicadores personalizados
        """
        try:
            spec = importlib.util.spec_from_file_location("custom_indicators", module_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["custom_indicators"] = module
            spec.loader.exec_module(module)
            
            if hasattr(module, 'CUSTOM_INDICATORS'):
                self.indicators.update(module.CUSTOM_INDICATORS)
                print(f"✅ {len(module.CUSTOM_INDICATORS)} indicadores personalizados cargados")
        except Exception as e:
            print(f"Error cargando indicadores personalizados: {e}")
    
    def calculate(self, indicator_name: str, **kwargs):
        """
        Calcula un indicador específico.
        
        Args:
            indicator_name: Nombre del indicador a calcular
            **kwargs: Argumentos adicionales para la función del indicador
            
        Returns:
            DataFrame con los resultados del indicador
        """
        if indicator_name not in self.indicators:
            raise ValueError(f"Indicador '{indicator_name}' no encontrado")
        
        print(f"Calculando indicador: {indicator_name}")
        result = self.indicators[indicator_name](self.data, **kwargs)
        
        if result is not None:
            print(f"{indicator_name}: {result.shape[0]} registros calculados")
        else:
            print(f"{indicator_name}: No se pudo calcular (variables faltantes)")
        
        return result
    
    def calculate_all(self, indicator_list: Optional[list] = None):
        """
        Calcula múltiples indicadores.
        
        Args:
            indicator_list: Lista de indicadores a calcular. Si es None, calcula todos.
            
        Returns:
            Diccionario con los resultados de cada indicador
        """
        if indicator_list is None:
            indicator_list = list(self.indicators.keys())
        
        results = {}
        for indicator in indicator_list:
            try:
                results[indicator] = self.calculate(indicator)
            except Exception as e:
                print(f"Error calculando {indicator}: {e}")
                results[indicator] = None
        
        return results
    
    def list_indicators(self):
        """Lista todos los indicadores disponibles"""
        return list(self.indicators.keys())