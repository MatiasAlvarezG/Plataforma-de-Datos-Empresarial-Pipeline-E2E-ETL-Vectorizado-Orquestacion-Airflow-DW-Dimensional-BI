import sys
from os.path import abspath, dirname, join
from pandas import testing
import unittest

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.ProductMethods import *
from ETL_tools.methods.GeneralTransformationMethods import metodos_generales


class TestProductMethods(unittest.TestCase):

    def setUp(self):
        self.dicc_df1 = {
                         'IdProducto': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                         'Producto': ['Motherboard Asus Modelo2', 'Mouse Logitech Modelo1', 'Teclado Dell Modelo4', 
                                      'Impresora HP Modelo5', 'Mouse Genius Modelo3', 'Teclado Asus Modelo1', 
                                      'Motherboard Dell Modelo5', 'Impresora Logitech Modelo2', 'Motherboard Asus Modelo4', 
                                      'Mouse Genius Modelo2'],
                         'Precio': [150.20, 87.50, 255.80, 402.30, 178.90, 92.60, 303.40, 468.10, 127.70, 54.30]
                        }
        self.df1 = DataFrame(self.dicc_df1)

    def tearDown(self):
        del self.dicc_df1
        del self.df1


    def test_desnormalizar_df_producto_parametros_None(self):
        with self.assertRaises(ValueError):
            desnormalizar_df_producto(None)

    def test_desnormalizar_df_producto_falta_de_columnas(self):
        with self.assertRaises(ValueError):
            desnormalizar_df_producto(self.df1)

    def test_desnormalizar_df_producto_desnormalizar(self):
        #Agrego columnas para utilizar el metodo
        self.df1['Componente'] = ''
        self.df1['Marca'] = ''
        self.df1['Modelo'] = ''

        self.df1 = metodos_generales(df=self.df1, 
                                     rellenar_nulos='Desconocido',
                                     transformaciones=True)
        
        desnormalizar_df_producto(self.df1)

        expected_result =  DataFrame({
                         'IdProducto': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                         'Producto': ['Motherboard Asus Modelo2', 'Mouse Logitech Modelo1', 'Teclado Dell Modelo4', 
                                      'Impresora HP Modelo5', 'Mouse Genius Modelo3', 'Teclado Asus Modelo1', 
                                      'Motherboard Dell Modelo5', 'Impresora Logitech Modelo2', 'Motherboard Asus Modelo4', 
                                      'Mouse Genius Modelo2'],
                         'Precio': [150.20, 87.50, 255.80, 402.30, 178.90, 92.60, 303.40, 468.10, 127.70, 54.30],
                         'Componente' : ['Motherboard','Mouse','Teclado','Impresora','Mouse','Teclado','Motherboard','Impresora','Motherboard','Mouse'],
                         'Marca' : ['Asus','Logitech','Dell','HP','Genius','Asus','Dell','Logitech','Asus','Genius'],
                         'Modelo' : ['Modelo2','Modelo1','Modelo4','Modelo5','Modelo3','Modelo1','Modelo5','Modelo2','Modelo4','Modelo2']
                        })
        
        expected_result = metodos_generales(df=expected_result, 
                                rellenar_nulos='Desconocido',
                                transformaciones=True)

        testing.assert_frame_equal(self.df1, expected_result)

if __name__ == '__main__':
    unittest.main()
