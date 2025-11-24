import sys
from os.path import abspath, dirname, join
import unittest

sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.SupplierMethods import *

class TestSupplierMethods(unittest.TestCase):
    def test_tipo_sociedad_con_vacias(self):
        tipos = encontrar_tipo_sociedad(Series(['', ' ', '  ']))
        self.assertEqual(list(tipos), ['Desconocido', 'Desconocido', 'Desconocido'])

    def test_tipo_sociedad_not_found_personalizado(self):
        tipos = encontrar_tipo_sociedad(Series(['Empresa X', 'Empresa Y']), not_found='No encontrada')
        self.assertEqual(list(tipos), ['No encontrada', 'No encontrada'])

    def test_tipo_sociedad_normal(self):
        tipos = encontrar_tipo_sociedad(Series(['Empresa SA', 'Empresa SRL', 'Empresa SAS']))
        self.assertEqual(list(tipos), ['SA', 'SRL', 'SAS'])

    def test_tipo_sociedad_None(self):
        with self.assertRaises(ValueError):
            encontrar_tipo_sociedad(None)

    def test_tipo_sociedad_no_pandas_Series(self):
        with self.assertRaises(TypeError):
            encontrar_tipo_sociedad(['Empresa X', 'Empresa Y'])

    def test_tipo_sociedad_lower_case_correctos(self):
        tipos = encontrar_tipo_sociedad(Series(['Empresa Sa', 'Empresa Srl', 'Empresa Sas']))
        self.assertEqual(list(tipos), ['SA', 'SRL', 'SAS'])

    def test_tipo_sociedad_mezcla_mayus_minus_correctos(self):
        tipos = encontrar_tipo_sociedad(Series(['EMPRESA Sa', 'empresa Srl', 'Empresa Sas']))
        self.assertEqual(list(tipos), ['SA', 'SRL', 'SAS'])

    def test_tipo_sociedad_con_caracteres_especiales_correctos(self):
        tipos = encontrar_tipo_sociedad(Series(['Empresa! Sa', 'Empresa!@ Srl', 'Empresa #Sas']))
        self.assertEqual(list(tipos), ['SA', 'SRL', 'SAS'])

    def test_tipo_sociedad_con_caracteres_todo_mayuscula(self):
        tipos = encontrar_tipo_sociedad(Series(['EMPRESA! SA', 'EMPRESA!@ SRL', 'EMPRESA #SAS']))
        self.assertEqual(list(tipos), ['SA', 'SRL', 'SAS'])
        
if __name__ == '__main__':
    unittest.main()
