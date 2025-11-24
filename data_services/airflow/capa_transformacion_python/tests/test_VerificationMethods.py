import sys
from os.path import abspath, dirname, join
import unittest

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.VerificationMethods import *

class TestVerificationMethods(unittest.TestCase):
    def setUp(self):
        self.df_integers = DataFrame({'A': [1, 2, 3, 4, 5]})
        self.df_floats = DataFrame({'B': [1.1, 2.2, 3.3, 4.4, 5.5]})
        self.df_strings = DataFrame({'C': ['a', 'b', 'c', 'd', 'e']})

    def tearDown(self):
        del self.df_integers
        del self.df_floats
        del self.df_strings

    def test_verificar_columna_int_exitoso(self):
        '''
        Verifica si los elementos de una columna son enteros.
        '''
        self.assertTrue(verificar_columna_int(self.df_integers, 'A'))

    def test_verificar_columna_int_con_try_cast_exitoso(self):
        '''
        Verifica si los elementos de una columna son enteros, intentando convertirlos en caso de ser posible.
        '''
        self.assertTrue(verificar_columna_int(self.df_floats, 'B', try_cast=True))

    def test_verificar_columna_int_fallido(self):
        '''
        Verifica si los elementos de una columna no son enteros.
        '''
        self.assertFalse(verificar_columna_int(self.df_strings, 'C'))

    def test_verificar_columna_int_con_try_cast_fallido(self):
        '''
        Verifica si los elementos de una columna no son enteros, sin intentar convertirlos.
        '''
        self.assertFalse(verificar_columna_int(self.df_strings, 'C', try_cast=True))

    def test_verificar_columna_int_parametro_df_none(self):
        '''
        Verifica si se lanza una excepción cuando el parámetro df es None.
        '''
        with self.assertRaises(ValueError):
            verificar_columna_int(None, 'A')

    def test_verificar_columna_int_parametro_df_no_dataframe(self):
        '''
        Verifica si se lanza una excepción cuando el parámetro df no es un DataFrame.
        '''
        with self.assertRaises(TypeError):
            verificar_columna_int('no soy un DataFrame', 'A')

    def test_verificar_columna_int_parametro_nombre_columna_none(self):
        '''
        Verifica si se lanza una excepción cuando el parámetro nombre_columna es None.
        '''
        with self.assertRaises(ValueError):
            verificar_columna_int(self.df_integers, None)

    def test_verificar_columna_int_parametro_nombre_columna_no_str(self):
        '''
        Verifica si se lanza una excepción cuando el parámetro nombre_columna no es una cadena.
        '''
        with self.assertRaises(TypeError):
            verificar_columna_int(self.df_integers, 123)

    def test_existencia_columnas_dataframe_nulo(self):
        '''
        Prueba el manejo de un DataFrame nulo.
        '''
        with self.assertRaises(ValueError):
            comprobar_existencia_columnas()

    def test_existencia_columnas_columna_nulas(self):
        '''
        Prueba si se lanza una excepcion cuando las columnas a buscar son nulas
        '''
        with self.assertRaises(ValueError):
            verificar_columna_int(self.df_integers, None)

    def test_existencia_columnas_correctas(self):
        '''
        Prueba si las columnas correctas existen en el DataFrame.
        '''
        columnas_correctas = ['A']
        resultado = comprobar_existencia_columnas(self.df_integers, columnas_correctas)
        self.assertTrue(resultado)

    def test_existencia_columnas_faltantes(self):
        '''
        Prueba si las columnas faltantes existen en el DataFrame.
        '''
        columnas_faltantes = ['B']
        resultado = comprobar_existencia_columnas(self.df_integers, columnas_faltantes)
        self.assertFalse(resultado)

    def test_existencia_columnas_con_log(self):
        '''
        Prueba si las columnas incorrectas existen en el DataFrame con log.
        '''
        columnas_incorrectas = ['B']
        resultado = comprobar_existencia_columnas(self.df_integers, columnas_incorrectas, log=True)
        self.assertIsInstance(resultado, list)

    def test_existencia_columnas_con_log_valores(self):
        '''
        Prueba si las columnas incorrectas existen en el DataFrame con log.
        '''
        columnas_incorrectas = ['B','A']
        resultado = comprobar_existencia_columnas(self.df_integers, columnas_incorrectas, log=True)
        self.assertEqual(['B'], resultado)

if __name__ == '__main__':
    unittest.main()
