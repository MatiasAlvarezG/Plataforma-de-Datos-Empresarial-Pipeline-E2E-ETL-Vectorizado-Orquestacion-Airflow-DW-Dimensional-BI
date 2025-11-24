import sys
from os.path import join, dirname, abspath
import unittest
from numpy import random

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.IdTransformationMethods import *

class TestIdTransformationMethods(unittest.TestCase):
    def test_convertir_caracteres_en_posiciones_consecutivas(self):
        self.assertIsNone(convertir_caracteres_en_posiciones_consecutivas(None))

        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas([]),
                         []
                         )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas([1,2,3]),
                         [1,2,3]
                         )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas([1,'a',3]),
                         [1,0,3]
                         )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas(['a',2,'c']),
                         [0,2,1]
                         )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas(['a',2,'a']),
                         [0,2,0]
                         )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas([ 0, 2, 3, 4, 5, 6, 7, 8, 9, 'hola', 11, 123, 1456, 'e', 'f', 'g' ]),
                         [0, 2, 3, 4, 5, 6, 7, 8, 9, 1, 11, 123, 1456, 10, 12, 13]
                         )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas(['a','b','c']),
                        [0,1,2]
                        )
        self.assertEqual(convertir_caracteres_en_posiciones_consecutivas(['a','b','c',12.4,18.4,18.9]),
                        [0,1,2,3,4,5]
                        )   

    def test_convertir_caracteres_en_posiciones_consecutivas_tipo_incorrecto(self):
        with self.assertRaises(TypeError):
            convertir_caracteres_en_posiciones_consecutivas(DataFrame({'A' : [1,2,3], 'B' : [0,0,0]}))
            convertir_caracteres_en_posiciones_consecutivas({'A' : [1,2,3], 'B' : [0,0,0]})
            convertir_caracteres_en_posiciones_consecutivas({1,2,3,4,5})
            convertir_caracteres_en_posiciones_consecutivas()

    def test_buscar_posicion_parametros_none(self):
        with self.assertRaises(ValueError):
            buscar_posicion_a_insertar(None, 10)
            buscar_posicion_a_insertar([1, 2, 3], None)

    def test_buscar_posicion_tipo_incorrecto(self):
        with self.assertRaises(TypeError):
            buscar_posicion_a_insertar(123, 10)
            buscar_posicion_a_insertar([1, 2, 3], 'texto')

    def test_buscar_posicion_ejemplos_documentacion(self):
        self.assertEqual(buscar_posicion_a_insertar([1, 24, 52, 100], 34), 2)
        self.assertEqual(buscar_posicion_a_insertar([10, 20, 30], 25), 2)
        self.assertEqual(buscar_posicion_a_insertar([1000, 2000, 3000], 500), 0)

    def test_buscar_posicion_pocos_elementos(self):
        self.assertEqual(buscar_posicion_a_insertar([1, 2, 3], 0), 0)
        self.assertEqual(buscar_posicion_a_insertar([1, 2, 4,5,6], 3), 2)
        self.assertEqual(buscar_posicion_a_insertar([1, 2], 3), 2)

    def test_buscar_posicion_mil_elementos(self):
        '''Como existe el valor debe retornar None'''
        lista = list(range(1000))
        self.assertIsNone(buscar_posicion_a_insertar(lista, 500), 500)

    def test_buscar_posicio_cinco_mil_elementos(self):
        lista = list(range(5000))
        lista.remove(2500)
        self.assertEqual(buscar_posicion_a_insertar(lista, 2500), 2500)

    def test_propagar_valores_parametros_none(self):
        with self.assertRaises(TypeError):
            propagar_valores(None, 'ID', 123, 9991)
            propagar_valores([DataFrame()], None, 123, 9991)

    def test_propagar_valores_tipo_incorrecto(self):
        with self.assertRaises(TypeError):
            propagar_valores('dataframe', 'ID', 123, 9991)
            propagar_valores([DataFrame()], 'ID', 'string', 9991)
            propagar_valores([DataFrame()], 'ID', 123, 'string')

    def test_propagar_valores_ejemplos_documentacion(self):
        # Creamos DataFrames de ejemplo
        df1 = DataFrame({'ID': [123, 456, 789], 'col2': [10, 20, 30]})
        df2 = DataFrame({'ID': [123, 456, 789], 'col2': [40, 50, 60]})
        df3 = DataFrame({'ID': [123, 456, 789], 'col2': [70, 80, 90]})
        
        # Copiamos los DataFrames para comparar después
        df1_copy = df1.copy()
        df2_copy = df2.copy()
        df3_copy = df3.copy()

        # Llamamos al método propagar_valores
        propagar_valores([df1, df2, df3], 'ID', 123, 9991)

        # Verificamos que los valores hayan sido actualizados correctamente
        self.assertTrue((df1['ID'] == df2['ID']).all())
        self.assertTrue((df1['ID'] == df3['ID']).all())
        self.assertTrue((df2['ID'] == df3['ID']).all())

        self.assertTrue((df1['col2'] == df1_copy['col2']).all())
        self.assertTrue((df2['col2'] == df2_copy['col2']).all())
        self.assertTrue((df3['col2'] == df3_copy['col2']).all())

        # Volvemos los DataFrames a su estado original
        df1['ID'] = df1_copy['ID']
        df2['ID'] = df2_copy['ID']
        df3['ID'] = df3_copy['ID']

    def test_propagar_valores_cinco_elementos(self):
        df1 = DataFrame({'ID': [123, 456, 789], 'col2': [10, 20, 30]})
        df2 = DataFrame({'ID': [123, 456, 789], 'col2': [40, 50, 60]})
        df3 = DataFrame({'ID': [123, 456, 789], 'col2': [70, 80, 90]})
        df4 = DataFrame({'ID': [123, 456, 789], 'col2': [100, 110, 120]})
        df5 = DataFrame({'ID': [123, 456, 789], 'col2': [130, 140, 150]})

        propagar_valores([df1, df2, df3, df4, df5], 'ID', 123, 9991)

        self.assertTrue((df1['ID'] == 9991).any())
        self.assertTrue((df2['ID'] == 9991).any())
        self.assertTrue((df3['ID'] == 9991).any())
        self.assertTrue((df4['ID'] == 9991).any())
        self.assertTrue((df5['ID'] == 9991).any())

    def setUp(self):
        # Crear DataFrames de ejemplo
        self.df_devolucion = DataFrame({'IdCliente': [1, 2, 3, 4, 5, 2, 6, 1, 3],
                                           'Devolucion': [True, False, False, False, False, False, False, False, True]})

        self.df_compra = DataFrame({'IdCliente': [3, 6, 2, 4, 5, 3, 2, 1, 1],
                                       'IdCompra': [['1AB', '01B'], ['00B'], ['DD1'], ['DD2', 'DD1', '1AB'],
                                                    ['BBA'], ['BCA', 'DD1'], ['1AB', 'DD2'], ['DD3'], ['00B', 'B10']]})

        self.df_info = DataFrame({'IdCliente': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                                      'Nombre': ['Matias Alvarez', 'NN1', 'NN2', 'NN3', 'NN4', 'NN5',
                                                 'NN6', 'NN7', 'NN8', 'NN9', 'NN10', 'NN11']})
        self.df = DataFrame({'id': [1, 2, 3, 4], 'direccion': ['aaa', 'bbb', 'ccc', 'aaa']})

    def tearDown(self):
        del self.df_compra
        del self.df_devolucion
        del self.df_info
        del self.df

    def test_renovar_id_parametros_none(self):
        with self.assertRaises(TypeError):
            renovar_id(None, 'IdCliente', 9, [self.df_devolucion, self.df_compra], 'IdCliente')
            renovar_id(self.df_info, 'NOEXISTE', 100, [self.df_devolucion, self.df_compra], 'IdCliente')

    def test_renovar_id_id_desactualizado(self):
        # Intentar renovar un ID que no existe
        renovar_id(self.df_info, 'IdCliente', 100, [self.df_devolucion, self.df_compra], 'IdCliente')
        self.assertTrue(100 not in self.df_info['IdCliente'])

    def test_renovar_id_actualizar_df(self):
        # Modificar el ID con valor 1
        renovar_id(df=self.df_info, 
                   columna_id_df='IdCliente', 
                   old_id=1, 
                   df_actualizar=[self.df_devolucion, self.df_compra], 
                   columna_df_actualizar='IdCliente')

        # Comprobar que los DataFrames se han actualizado correctamente
        self.assertTrue((self.df_devolucion['IdCliente'] == [13, 2, 3, 4, 5, 2, 6, 13, 3]).all())
        self.assertTrue((self.df_compra['IdCliente'] == [3, 6, 2, 4, 5, 3, 2, 13, 13]).all())
        self.assertTrue((self.df_info['IdCliente'] == [13, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).all())

    def test_descartar_registros_duplicados_con_id_distintos_parametros_none(self):
        with self.assertRaises(ValueError):
            descartar_registros_duplicados_con_id_distintos(None, 'direccion', 'id')
            descartar_registros_duplicados_con_id_distintos(self.df, None, 'id')
            descartar_registros_duplicados_con_id_distintos(self.df, 'direccion', None)

    def test_descartar_registros_duplicados_con_id_distintos_descartar_duplicados(self):
        # Verificar que la columna 'id_Descartado' no existe
        self.assertFalse('id_Descartado' in self.df.columns)

        # Verificar que la columna 'id_Correcto' no existe
        self.assertFalse('id_Correcto' in self.df.columns)

        # Procesar los duplicados
        descartar_registros_duplicados_con_id_distintos(self.df, 'direccion', 'id')

        # Verificar que la columna 'id_Descartado' ahora existe
        self.assertTrue('id_Descartado' in self.df.columns)

        # Verificar que la columna 'id_Correcto' ahora existe
        self.assertTrue('id_Correcto' in self.df.columns)

        # Verificar que se marcó correctamente el registro duplicado
        self.assertTrue((self.df['id_Descartado'] == [0, 0, 0, 1]).all())

        # Verificar que se marcó correctamente el ID correcto
        self.assertTrue((self.df['id_Correcto'] == [1, 2, 3, 1]).all())

    def test_descartar_registros_duplicados_con_id_distintos_actualizar_duplicados(self):
        # Crear DataFrame adicional
        df_actualizar = DataFrame({'id': [3, 4, 5], 'telefono': [111, 222, 333]})

        # Procesar los duplicados y actualizar
        descartar_registros_duplicados_con_id_distintos(self.df, 'direccion', 'id', df_actualizar, 'id')

        # Verificar que se actualizó correctamente el ID duplicado
        self.assertTrue((df_actualizar['id'] == [3, 1, 5]).all())
        # original df: DataFrame({'id': [1, 2, 3, 4], 'direccion': ['aaa', 'bbb', 'ccc', 'aaa']})

if __name__=='__main__':
    unittest.main()
