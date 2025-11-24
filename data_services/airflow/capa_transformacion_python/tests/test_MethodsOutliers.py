import sys
from os.path import abspath, dirname, join
import unittest 

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.MethodsOutliers import *


class TestMethodsOutliers(unittest.TestCase):

    def setUp(self):
        self.df = DataFrame({'Edad': [20, 25, 30, 35, 40, 45, 50, 55, 60]})

        self.df2 = DataFrame({
            'A': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            'B': [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            'C': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        })

        self.df3 = DataFrame({'Edad' : [22,22,23,23,23,23,26,27,27,28,30,30,30,30,31,32,33,34,80]})

        self.df_outlier = DataFrame({
            'Idproducto': [1, 2, 3, 4, 5],
            'Outlier': [0, 0, 1, 0, 0],
            'Fecha_Año': [2019, 2019, 2019, 2020, 2020],
            'Precio': [100, 200, 300, 400, 500]
        })
    
        self.df_producto = DataFrame({
            'IdProducto': [1, 2, 3, 4, 5],
            'Precio': [150, 250, 350, 450, 550]
        })

    def tearDown(self):
        del self.df
        del self.df2
        del self.df3
        del self.df_outlier
        del self.df_producto

    def test_calcular_quartiles_calculo_correcto(self):
        # Prueba que verifica el cálculo correcto de los cuartiles
        resultado_esperado = (30, 40, 50)
        self.assertEqual(calcular_quartiles(self.df, 'Edad', [0.25, 0.5, 0.75]), resultado_esperado)

    def test_calcular_quartiles_valores_fuera_de_rango(self):
        # Prueba que verifica si el método maneja correctamente los valores de cuartiles fuera del rango [0, 1]
        resultado_esperado = (20, 40, 60)  # Se espera que todos los cuartiles no se puedan calcular
        self.assertEqual(calcular_quartiles(self.df, 'Edad', [-1, 0.5, 1.5]), resultado_esperado)

    def test_calcular_quartiles_parametros_none(self):
        # Prueba que verifica si se manejan correctamente los parámetros None
        with self.assertRaises(ValueError):
            calcular_quartiles(None, 'Edad', [0.25, 0.5, 0.75])
            calcular_quartiles(self.df, None, [0.25, 0.5, 0.75])

    def test_calcular_quartiles_df_incorrecto(self):
        # Prueba que verifica si se maneja correctamente un DataFrame incorrecto
        with self.assertRaises(TypeError):
            calcular_quartiles('no un DataFrame', 'Edad', [0.25, 0.5, 0.75])

    def test_calcular_quartiles_valores_por_defecto(self):
        # Prueba que verifica si el método devuelve una tupla vacía cuando no se pasan valores de cuartiles
        self.assertEqual(calcular_quartiles(self.df, 'Edad'), ())

    def test_calcular_quartiles_valores_invalidos(self):
        # Prueba que verifica si se manejan correctamente los valores de cuartiles inválidos
        resultado_esperado = (-1, -1, -1)
        self.assertEqual(calcular_quartiles(self.df, 'Edad', ['a', 'b', 'c']), resultado_esperado)

    def test_encontrar_outliers_IQR_outliers_sin_filtrar(self):
        outliers = encontrar_outliers_IQR(self.df2, 'A')
        self.assertEqual(len(outliers), 0)

    def test_encontrar_outliers_IQR_outliers_con_filtrado(self):
        outliers = encontrar_outliers_IQR(self.df3, 'Edad', filtrar=True, valor=20)
        self.assertEqual(len(outliers), 1)

    def test_encontrar_outliers_IQR_quartiles_custom(self):
        outliers = encontrar_outliers_IQR(self.df2, 'C', quartiles=[0.1, 0.9])
        self.assertEqual(len(outliers), 0)
        # resultado_esperado = DataFrame({'A' : [], 'B' : [], 'C' : []})
        # testing.assert_frame_equal(outliers, resultado_esperado)

    def test_encontrar_outliers_IQR_columna_inexistente(self):
        with self.assertRaises(KeyError):
            encontrar_outliers_IQR(self.df, 'X')

    def test_encontrar_outliers_IQR_dataframe_vacio(self):
        df_vacio = DataFrame()
        with self.assertRaises(KeyError):
            encontrar_outliers_IQR(df_vacio, 'A')

    def test_encontrar_outliers_IQR_parametros_nulos(self):
        with self.assertRaises(ValueError):
            encontrar_outliers_IQR(None, 'A')
        with self.assertRaises(ValueError):
            encontrar_outliers_IQR(self.df2, None)

    def test_encontrar_outliers_IQR_tipos_no_validos(self):
        with self.assertRaises(TypeError):
            encontrar_outliers_IQR("No es un DataFrame", 'A')
        with self.assertRaises(KeyError):
            encontrar_outliers_IQR(self.df2, 123)


if __name__ == '__main__':
    unittest.main()
