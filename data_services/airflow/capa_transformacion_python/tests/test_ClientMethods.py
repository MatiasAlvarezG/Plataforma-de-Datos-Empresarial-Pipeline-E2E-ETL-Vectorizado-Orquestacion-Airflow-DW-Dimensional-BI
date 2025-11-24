import sys
import unittest
from pandas import DataFrame
from os.path import abspath, join, dirname
sys.path.append(abspath(join(dirname(__file__), '..', 'src'))) 
from ETL_tools.methods.ClientMethods import *

class TestClientMethods(unittest.TestCase):
    def test_obtener_grupo_etario_None(self):
        '''
        Se comprueba la función: obtener_grupo_etario() para un caso incorrecto de uso.
        Entrada:
            1) edad=None, error='Desconocido' (default)
            2) edad=None, error='Error' 
        Salida esperada:
            1) 'Desconocido'
            2) 'Error'
        '''
        self.assertEqual(obtener_grupo_etario(None),'Desconocido')
        self.assertEqual(obtener_grupo_etario(None,'Error'),'Error')

    def test_obtener_grupo_etario_correctos(self):
        '''
        Se comprueba la función: obtener_grupo_etario() para un caso correcto de uso.
        Entrada:
            1) edad=1, error='Desconocido' (default)
            2) edad='1' (str), error='Desconocido' (default)
            3) edad=11, error='Desconocido' (default)
            4) edad=12, error='Desconocido' (default)
            5) edad=40, error='Desconocido' (default)
            6) edad=35, error='Desconocido' (default)
            7) edad='35' (str), error='Desconocido' (default)
            8) edad=39, error='Desconocido' (default)
            9) edad=18, error='Desconocido' (default)
            10) edad=17, error='Desconocido' (default)
            11) edad='17' (str), error='Desconocido' (default)
            12) edad=0, error='Desconocido' (default)
            13) edad=98, error='Desconocido' (default)
        Salida esperada:
            1) 'Niñez'
            2) 'Niñez'
            3) 'Niñez'
            4) 'Adolescencia'
            5) 'Adultez'
            6) 'Adultez'
            7) 'Adultez'
            8) 'Adultez'
            9) 'Juventud'
            10) 'Adolescencia'
            11) 'Adolescencia'
            12) 'Niñez'
            13) 'Vejez'
        Observación:
            1) Debe devolver el valor correcto en caso de pasarse el parámetro de tipo cadena que hace referencia a un número de edad válido.
        '''
        self.assertEqual(obtener_grupo_etario(1),'Niñez')
        self.assertEqual(obtener_grupo_etario('1'),'Niñez')
        self.assertEqual(obtener_grupo_etario(11),'Niñez')
        self.assertEqual(obtener_grupo_etario(12), 'Adolescencia')
        self.assertEqual(obtener_grupo_etario(40), 'Adultez')
        self.assertEqual(obtener_grupo_etario(35), 'Adultez')
        self.assertEqual(obtener_grupo_etario('35'), 'Adultez')
        self.assertEqual(obtener_grupo_etario(39), 'Adultez')
        self.assertEqual(obtener_grupo_etario(18), 'Juventud')
        self.assertEqual(obtener_grupo_etario(17), 'Adolescencia')
        self.assertEqual(obtener_grupo_etario('17'), 'Adolescencia')
        self.assertEqual(obtener_grupo_etario(0), 'Niñez')
        self.assertEqual(obtener_grupo_etario(98), 'Vejez')

    def test_obtener_grupo_etario_incorrectos(self):
        '''
        Se comprueba la función: grupo_etario(edad, error) para un uso incorrecto.
        Entrada:
            1) edad='ejemplo' (cadena no númerica), error='Desconocido' (default)
            2) edad={'a' : 123} (Dict), error='Desconocido' (default)
            3) edad=object() (object), error='Desconocido' (default)
            4) edad=DataFrame() (Dataframe), error='Desconocido' (default)
            5) edad=set() (Set), error='Desconocido' (default)
            6) edad='ejemplo' (cadena no númerica), error='-1' 
            7) edad={'a' : 123} (Dict), error='ERROR'
            8) edad=object(), error=-1)
        Salida esperada:
            1) 'Desconocido'
            2) 'Desconocido'
            3) 'Desconocido'
            4) 'Desconocido'
            5) 'Desconocido'
            6) '-1'
            7) 'ERROR'
            8) -1
        '''
        self.assertEqual(obtener_grupo_etario('ejemplo'), 'Desconocido')
        self.assertEqual(obtener_grupo_etario({'a' : 123}), 'Desconocido')
        self.assertEqual(obtener_grupo_etario(object()), 'Desconocido')
        self.assertEqual(obtener_grupo_etario(DataFrame()), 'Desconocido')
        self.assertEqual(obtener_grupo_etario(set()), 'Desconocido')

        self.assertEqual(obtener_grupo_etario('ejemplo',error='-1'), '-1')
        self.assertEqual(obtener_grupo_etario({'a' : 123},error='ERROR'), 'ERROR')
        self.assertEqual(obtener_grupo_etario(object(),error=-1), -1)


    def test_separar_nombre_y_apellido_parametro_none(self):
        '''
        Se comprueba la función: separar_nombre_y_apellido(PERSONA:list) con un parámetro None
        Entrada:
            1) PERSONA=None
        Salida esperada:
            1) NoneType
        '''
        self.assertIsNone(separar_nombre_y_apellidos(None))

    def test_separar_nombre_y_apellido_parametro_erroneo(self):
        '''
        Se comprueba la función: separar_nombre_y_apellido(PERSONA:list) con parámetros que no son de tipo lista
        Entrada:
            Cadenas, tuplas, dict y conjuntos
        Salida esperada:
            Raise: TypeError
        '''
        with self.assertRaises(TypeError):
            separar_nombre_y_apellidos(('Matias','Alvarez'))
            separar_nombre_y_apellidos({'Matias' : 'Alvarez'})
            separar_nombre_y_apellidos({'Alvarez','Matias'})
            separar_nombre_y_apellidos('Matias Alvarez')


    def test_separar_nombre_y_apellido_correcto(self):
        '''
        Se comprueba la función: separar_nombre_y_apellido() con parámetro correcto
        Entrada:
            1) ['Matias','Alvarez']
            2) ['Alvarez', 'Matias']
            3) ['De','La','Fuente','Alberto']
            4) ['Alfredo','Ruis','De', 'Los', 'Andes']
            5) ['Lionel', 'Messi']
            6) ['Messi', 'Lionel']
        Salida esperada:
            1) ['Matias'],['Alvarez']
            2) ['Matias'],['Alvarez']
            3) ['Alberto'], ['De La Fuente']
            4) ['Alfredo'], ['Ruis', 'De Los Andes']
            5) ['Lionel'], ['Messi']
            6) ['Lionel'], ['Messi']
        '''
        self.assertEqual(separar_nombre_y_apellidos(['Matias','Alvarez']), (['Matias'],['Alvarez']))
        self.assertEqual(separar_nombre_y_apellidos(['Alvarez', 'Matias']), (['Matias'],['Alvarez']))
        self.assertEqual(separar_nombre_y_apellidos(['De','La','Fuente','Alberto']),
                        (['Alberto'], ['De La Fuente'])
                        )
        self.assertEqual(separar_nombre_y_apellidos(['Alfredo','Ruis','De', 'Los', 'Andes']),
                          (['Alfredo'], ['Ruis', 'De Los Andes'])
                        )
        self.assertEqual(separar_nombre_y_apellidos(['Lionel', 'Messi']),(['Lionel'], ['Messi']))
        self.assertEqual(separar_nombre_y_apellidos(['Messi', 'Lionel']),(['Lionel'], ['Messi']))
        
if __name__ == '__main__':
    unittest.main()
