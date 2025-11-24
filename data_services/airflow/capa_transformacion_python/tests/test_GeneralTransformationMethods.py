import sys
from os.path import abspath, dirname, join
from pandas import testing
from numpy import NaN
import unittest

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.GeneralTransformationMethods import *

class TestGeneralTransformationMethods(unittest.TestCase):
    def setUp(self):
        self.serie1 = Series([1, 2, 3, 4, NaN])
        self.serie2 = Series([1, 2, NaN, NaN, 5])
        self.serie3 = Series([NaN, 2, NaN, 4, NaN])
        self.serie4 = Series([NaN, NaN, NaN, 4, NaN])

        self.df1 = DataFrame({ 'A' : ['MATIAS ALVAREZ','CARLOS ALBERTO Fernandez', 'mAtías      AlVAREZ  G ','MATIAS ALVAREZ'],
                               'B' : [1,2,3,1], 
                               'C' : [1,NaN,NaN,1], 
                               'D' : [NaN,NaN,NaN,NaN], 
                               'E' : [4,3,2,4] })
        
        self.df2 = DataFrame({ 'A' : [1,2,3], 'B' : [1,2,3], 'C' : [1,2,3], 'D' : [1,2,3], 'E' : [1,2,3] })

    def tearDown(self):
        del self.serie1 
        del self.serie2
        del self.serie3 
        del self.serie4         

        del self.df1 
        del self.df2

    def test_detectar_nulos_none(self):
        self.assertEqual(detectar_nulos(None, strict_mode=False), {})

    def test_detectar_nulos_none_strict_mode(self):
        with self.assertRaises(ValueError):
            detectar_nulos(None, strict_mode=True)

    def test_detectar_nulos_otro_tipo_dato_strict_mode(self):
        with self.assertRaises(TypeError):
            detectar_nulos({1,2,3}, strict_mode=True)
            detectar_nulos((1,2,3), strict_mode=True)
            detectar_nulos(object(), strict_mode=True)


    def test_detectar_nulos_series(self):
        self.assertEqual(detectar_nulos(self.serie1), 1)
        self.assertEqual(detectar_nulos(self.serie2), 2)
        self.assertEqual(detectar_nulos(self.serie3), 3)
        self.assertEqual(detectar_nulos(self.serie4), 4)
        self.assertEqual(detectar_nulos(self.df1['A']), 0)


    def test_detectar_nulos_dataframe(self):
        self.assertEqual(detectar_nulos(self.df1), {'C' : 2, 'D' : 4})
        self.assertEqual(detectar_nulos(self.df2), {})

    
    def test_detectar_nulos_strict_mode(self):
        with self.assertRaises(ValueError):
            detectar_nulos(None, strict_mode=True)
    
    # Test para eliminar_espacios
    def test_eliminar_espacios_none(self):
        result = eliminar_espacios(None)
        self.assertIsNone(result)
    
    def test_eliminar_espacios_cadena(self):
        msg1 = "  esto   es un        ejemplo  "
        msg2 = "                    "
        self.assertEqual(eliminar_espacios(msg1, strict_mode=False), "esto es un ejemplo")
        self.assertEqual(eliminar_espacios(msg2, strict_mode=False), "")
            
    # Test para metodos_generales
    def test_metodos_generales_none(self):
        with self.assertRaises(ValueError):
            metodos_generales(None, rellenar_nulos=-1, transformaciones=False)

    def test_metodos_generales_con_transformacion(self):
        result = metodos_generales(self.df1, rellenar_nulos=-1, transformaciones=True)
        expected_result = DataFrame({ 'A' : ['Matias Alvarez', 'Carlos Alberto Fernandez', 'Matias Alvarez G'],
                                      'B' : [1, 2, 3],
                                      'C' : [1.0, -1.0, -1.0],
                                      'D' : [-1.0, -1.0, -1.0],
                                      'E' : [4, 3, 2]})
        testing.assert_frame_equal(result, expected_result)

    def test_metodos_generales_sin_transformacion(self):
        result = metodos_generales(self.df1, rellenar_nulos=-1, transformaciones=False)
        expected_result = DataFrame({ 'A' : ['MATIAS ALVAREZ', 'CARLOS ALBERTO Fernandez', 'mAtías      AlVAREZ  G '],
                                      'B' : [1, 2, 3],
                                      'C' : [1.0, -1.0, -1.0],
                                      'D' : [-1.0, -1.0, -1.0],
                                      'E' : [4, 3, 2]})
        testing.assert_frame_equal(result, expected_result)

    
    def test_normalizar_valores_none(self):
        serie = Series(["apple", "banana", "kiwi"])
        with self.assertRaises(ValueError):
            normalizar_valores(serie, None)
            normalizar_valores(None, {'A' : 'a'})
            normalizar_valores(None,None)
            normalizar_valores(Series(['caracter']), None)
        
    def test_normalizar_valores_tipo_incorrecto_serie(self):
        # Prueba para tipo incorrecto en serie
        with self.assertRaises(TypeError):
            normalizar_valores('no_serie', {'caracter': 'letra'})
        
    def test_normalizar_valores_tipo_incorrecto_diccionario(self):
        # Prueba para tipo incorrecto en diccionario
        with self.assertRaises(TypeError):
            normalizar_valores(Series(['caracter']), 'no_diccionario')
        
    def test_normalizar_valores_transformacion_simple(self):
        # Prueba para transformación simple
        serie = Series(['hola mundo', 'caracter especial'])
        diccionario = {'mundo': 'world', 'caracter': 'letter'}
        
        normalizar_valores(serie, diccionario)
        
        expected_result = Series(['hola world', 'letter especial'])
        testing.assert_series_equal(serie, expected_result)
        
    def test_normalizar_valores_sin_coincidencias(self):
        # Prueba para serie sin coincidencias en el diccionario
        serie = Series(['hola mundo', 'caracter especial'])
        diccionario = {'perro': 'dog', 'gato': 'cat'}
        
        normalizar_valores(serie, diccionario)
        
        expected_result = Series(['hola mundo', 'caracter especial'])
        testing.assert_series_equal(serie, expected_result)

        normalizar_valores(serie, diccionario)
        
        expected_result = Series(['hola mundo', 'caracter especial'])
        testing.assert_series_equal(serie, expected_result)
        
    def test_normalizar_valores_con_numeros(self):
        # Prueba para serie con valores numéricos
        serie = Series(['123 abc', 'abc 456'])
        diccionario = {'123': 'one_two_three', '456': 'four_five_six'}
        
        normalizar_valores(serie, diccionario)
        
        expected_result = Series(['one_two_three abc', 'abc four_five_six'])
        testing.assert_series_equal(serie, expected_result)
        
    def test_normalizar_valores_con_caracteres_repetidos(self):
        # Prueba para serie con caracteres repetidos
        serie = Series(['hello', 'woooow'])
        diccionario = {'o': 'ooo', 'w': 'w_w_w'}
        
        normalizar_valores(serie, diccionario)
        
        expected_result = Series(['hello', 'woooow'])
        testing.assert_series_equal(serie, expected_result)

    def test_almacenar_incosistencia_none(self):
        mask = self.df1['A'].eq('MATIAS ALVAREZ')
        self.df1['Error'] = 'Sin errores'
        with self.assertRaises(ValueError):
            almacenar_incosistencia(None,'A', mask, 'Error')
            almacenar_incosistencia(self.df1,None, mask, 'Error')
            almacenar_incosistencia(self.df1,'A', None, 'Error')
            almacenar_incosistencia(self.df1,'A', mask, None)
            almacenar_incosistencia(None,None,None,None)

    def test_almacenar_incosistencia_tipo_incorrecto(self):
        self.df1['Error'] = 'Sin errores'
        mask = self.df1['A'].eq('MATIAS ALVAREZ')
        serie = self.df1['A']
        with self.assertRaises(TypeError):
            almacenar_incosistencia(serie,'A', mask, 'Error')
            almacenar_incosistencia(serie,'A', self.df2, 'Error')

    def test_almacenar_incosistencia_tipo_incorrecto(self):
        self.df1['Error'] = 'Sin errores'
        mask = self.df1['A'].eq('MATIAS ALVAREZ')
        columna = 'NO_EXISTE'
        with self.assertRaises(KeyError):
            almacenar_incosistencia(self.df1,columna, mask, 'Error')
            

    def test_almacenar_incosistencia_correcto(self):
        df = DataFrame({'A': [0, 1], 'B': [1, 1], 'Error': [0, 0]})
        mask = df['A'] == 1

        #Como no paso un slice como parametro df puedo utilizar el metodo como un comando.
        almacenar_incosistencia(df, 'A', mask, 'Error')
        expected_result = DataFrame({'A': [0, 1], 'B': [1, 1], 'Error': [0, 1]})
        testing.assert_frame_equal(df, expected_result)

        self.df1['Error'] = 'Sin errores'
        mask = self.df1['A'].eq('MATIAS ALVAREZ')
        #Como paso un slice como parametro df debo almacenar el resultado.
        self.df1[['A','Error']] = almacenar_incosistencia(self.df1[['A','Error']],'A',mask, 'Error')  
        expected_result =  DataFrame(
            { 'A' : ['MATIAS ALVAREZ', 'CARLOS ALBERTO Fernandez', 
                     'mAtías      AlVAREZ  G ', 'MATIAS ALVAREZ'],
              'Error' : ['MATIAS ALVAREZ', 'Sin errores',
                         'Sin errores', 'MATIAS ALVAREZ'] 
            } 
        )
        testing.assert_frame_equal(self.df1[['A','Error']], expected_result)


        
if __name__ == '__main__':
    unittest.main()
