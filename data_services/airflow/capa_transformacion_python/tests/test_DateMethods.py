import sys
from os.path import abspath, dirname, join
import unittest
sys.path.append(abspath(join(dirname(__file__), '..', 'src')))
from ETL_tools.methods.DateMethods import *


class TestDateMethods(unittest.TestCase):
    def setUp(self):
        self.fila1 = {'Fecha_Año': 2022, 'Fecha_Mes': 6, 'Fecha_Dia': 12}
        self.fila2 = {'Año': '2022', 'Mes': '09', 'Dia': '25'}
        self.fila3 = {'A': 2022, 'M': 6, 'D': 29}

        self.col1=['Fecha_Año','Fecha_Mes','Fecha_Dia']
        self.col2=['Año','Mes','Dia']
        self.col3=['A','M','D']

    def tearDown(self):
        del self.fila1
        del self.fila2
        del self.fila3

        del self.col1
        del self.col2
        del self.col3

    def test_extraer_partes_fecha_formato_None(self):
        self.assertEqual(extraer_partes_fecha('12/6/2022',None), ('-1','-1','-1'))
        self.assertEqual(extraer_partes_fecha('1-6-2022',None), ('-1','-1','-1'))
        self.assertEqual(extraer_partes_fecha('1262022',None), ('-1','-1','-1'))

    def test_extraer_partes_fecha_formato_mal_pasado(self):
        self.assertEqual(extraer_partes_fecha('12/6/2022','ADM'), ('-1', '-1', '-1'))
        self.assertEqual(extraer_partes_fecha('12/6/2022','AMD'), ('-1', '-1', '-1'))
        self.assertEqual(extraer_partes_fecha('12/30/2022','DMA'), ('-1', '-1', '-1'))
        self.assertEqual(extraer_partes_fecha('30/3/2022','MDA'), ('-1', '-1', '-1'))

    def test_extraer_partes_fecha_formato_valido_minuscula(self):
        self.assertEqual(extraer_partes_fecha('12/6/2022','dma'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('12/6/2022','dMa'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('12/6/2022','dmA'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('12/6/2022','Dma'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('6/12/2022','mda'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('6/12/2022','mDa'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('6/12/2022','mdA'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('6/12/2022','Mda'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('2022/06/1','amd'), (1, 6, 2022))

    def test_extraer_partes_fecha_parametros_none_strict_mode(self):
        with self.assertRaises(ValueError):
            extraer_partes_fecha(None,strict_mode=True)
            extraer_partes_fecha(fecha=(),strict_mode=True)

    def test_validar_fecha(self):
        #Casos Correctos
        self.assertTrue(validar_fecha('12/06/2022',formato='DMA',strict_mode=False))
        self.assertTrue(validar_fecha('12-06-2022',formato='DMA',strict_mode=False))
        self.assertTrue(validar_fecha('12062022',formato='DMA',strict_mode=False))
        self.assertTrue(validar_fecha('2022/06/12',formato='AMD',strict_mode=False))
        self.assertTrue(validar_fecha('2022-06-12',formato='AMD',strict_mode=False))
        self.assertTrue(validar_fecha('2022/12/06',formato='ADM',strict_mode=False))
        self.assertTrue(validar_fecha('2022-12-06',formato='ADM',strict_mode=False))
        self.assertTrue(validar_fecha('06/12/2022',formato='MDA',strict_mode=False))
        self.assertTrue(validar_fecha('06-12-2022',formato='MDA',strict_mode=False))
        self.assertTrue(validar_fecha('06122022',formato='MDA',strict_mode=False))
        self.assertTrue(validar_fecha('6-12-2022',formato='MDA',strict_mode=False))

        #Casos Errados
        self.assertFalse(validar_fecha('30/06/2024',formato='ADM',strict_mode=False))
        self.assertFalse(validar_fecha('31/06/2024',formato='DMA',strict_mode=False))
        self.assertFalse(validar_fecha('30$06/2024',formato='DMA',strict_mode=False))
        self.assertFalse(validar_fecha('30$06|2024',formato='DMA',strict_mode=False))
        self.assertFalse(validar_fecha('2024/06/31',formato='AMD',strict_mode=False))
        self.assertFalse(validar_fecha('202206125',formato='AMD',strict_mode=False))
        self.assertFalse(validar_fecha('2024/31/06',formato='ADM',strict_mode=False))
        self.assertFalse(validar_fecha('202206125',formato='ADM',strict_mode=False))
        #No puede resolver porque no hay un separador
        self.assertFalse(validar_fecha('6122022',formato='MDA',strict_mode=False))


    def test_es_bisiesto(self):
        #Casos correctos
        self.assertTrue(es_bisiesto(2024,strict_mode=False))
        self.assertTrue(es_bisiesto('2024',strict_mode=False))
        self.assertTrue(es_bisiesto(2024.0,strict_mode=False))
        self.assertTrue(es_bisiesto(2020,strict_mode=False))
        self.assertTrue(es_bisiesto(2016,strict_mode=False))
        self.assertTrue(es_bisiesto(2012,strict_mode=False))
        self.assertTrue(es_bisiesto(2004,strict_mode=False))
        self.assertTrue(es_bisiesto(2000,strict_mode=False))
        self.assertTrue(es_bisiesto(400,strict_mode=False))
        #Casos Falsos/Erroneos
        self.assertFalse(es_bisiesto(None,strict_mode=False))
        self.assertFalse(es_bisiesto({2024},strict_mode=False))
        self.assertFalse(es_bisiesto({'2024' : 2024},strict_mode=False))
        self.assertFalse(es_bisiesto(2022,strict_mode=False))
        self.assertFalse(es_bisiesto(2021,strict_mode=False))
        self.assertFalse(es_bisiesto(2019,strict_mode=False))
        self.assertFalse(es_bisiesto(2018,strict_mode=False))
        self.assertFalse(es_bisiesto(2017,strict_mode=False))
        self.assertFalse(es_bisiesto('2017',strict_mode=False))
        self.assertFalse(es_bisiesto(201.7,strict_mode=False))

    def test_es_bisiesto_parametros_strict_mode(self):
        with self.assertRaises(ValueError):
            es_bisiesto(None,strict_mode=True)
            es_bisiesto('None',strict_mode=True)
            es_bisiesto({1,2,3},strict_mode=True)
            es_bisiesto({'A' : 1, 'B' : 2, 'C' : 3},strict_mode=True)
            es_bisiesto(3.14,strict_mode=True)
            es_bisiesto(object(),strict_mode=True)

    def test_validar_fecha_entera_partes(self):
        self.assertTrue(validar_fecha_entera_partes(12, 6, 2024))
        self.assertFalse(validar_fecha_entera_partes(31, 6, 2024))
        self.assertTrue(validar_fecha_entera_partes(29, 2, 2024)) 
        self.assertFalse(validar_fecha_entera_partes(-1, 6, 2024))
        self.assertFalse(validar_fecha_entera_partes(30, 2, 2024))

    def test_validar_fecha_entera_partes_parametros_none_strict_mode(self):
        with self.assertRaises(ValueError):
            validar_fecha_entera_partes(None, None, None,strict_mode=True)
            validar_fecha_entera_partes(1, None, 2015,strict_mode=True)
            validar_fecha_entera_partes(None, 9, None,strict_mode=True)

    def test_validar_fecha_entera_partes_parametros_none(self):
        self.assertFalse(validar_fecha_entera_partes(None, None, None,strict_mode=False))
        self.assertFalse(validar_fecha_entera_partes(None, 6, 2019,strict_mode=False))
        self.assertFalse(validar_fecha_entera_partes(1, None, 2015,strict_mode=False))
        self.assertFalse(validar_fecha_entera_partes(None, 9, None,strict_mode=False))

    def test_extraer_partes_fecha_correcto(self):
        self.assertEqual(extraer_partes_fecha('12/6/2022','DMA'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('6/12/2022','MDA'), (12, 6, 2022)) 
        self.assertEqual(extraer_partes_fecha('2022/12/6','ADM'), (12, 6, 2022))
        self.assertEqual(extraer_partes_fecha('2022/6/12','AMD'), (12, 6, 2022))

    def test_transformar_fecha_por_formato_por_fila_parametros_none(self):
        with self.assertRaises(ValueError):
            transformar_fecha_por_formato_por_fila(None)

    def test_transformar_fecha_por_formato_por_fila(self):
        self.assertEqual(transformar_fecha_por_formato_por_fila(fila=self.fila1,
                                                                formato='AMD',
                                                                sep='-',
                                                                columnas=self.col1),
                        '2022-6-12'
                        )

    def test_obtener_id_fecha_fila_parametros_none(self):
        with self.assertRaises(ValueError):
            obtener_id_fecha_fila(None)

    def test_obtener_id_fecha_fila(self):
        self.assertEqual(obtener_id_fecha_fila(self.fila1,formato='MDA',columnas=self.col1), '06122022')#'6122022')
        self.assertEqual(obtener_id_fecha_fila(self.fila1,formato='DMA',columnas=self.col1), '12062022')#'1262022')
        self.assertEqual(obtener_id_fecha_fila(self.fila1,formato='ADM',columnas=self.col1), '20221206')#'2022126')
        self.assertEqual(obtener_id_fecha_fila(self.fila1,formato='Amd',columnas=self.col1), '20220612')#'2022612')

        self.assertEqual(obtener_id_fecha_fila(self.fila2,formato='MDA',columnas=self.col2), '09252022')#'09252022')
        self.assertEqual(obtener_id_fecha_fila(self.fila2,formato='DMA',columnas=self.col2), '25092022')#'25092022')
        self.assertEqual(obtener_id_fecha_fila(self.fila2,formato='ADM',columnas=self.col2), '20222509')#'20222509')
        self.assertEqual(obtener_id_fecha_fila(self.fila2,formato='Amd',columnas=self.col2), '20220925')#'20220925')

        self.assertEqual(obtener_id_fecha_fila(self.fila3,formato='MDA',columnas=self.col3), '06292022')#'6292022')
        self.assertEqual(obtener_id_fecha_fila(self.fila3,formato='DMA',columnas=self.col3), '29062022')#'2962022')
        self.assertEqual(obtener_id_fecha_fila(self.fila3,formato='ADM',columnas=self.col3), '20222906')#'2022296')
        self.assertEqual(obtener_id_fecha_fila(self.fila3,formato='Amd',columnas=self.col3), '20220629')#'2022629')


    def test_obtener_mes_tostring_parametros_none(self):
        with self.assertRaises(ValueError):
            obtener_mes_tostring(None,strict_mode=True)

    def test_obtener_periodo_correcto(self):
        año = self.fila1[self.col1[0]]
        mes = self.fila1[self.col1[1]]
        resultado = '201206'

        self.assertEqual(obtener_periodo(año, mes,strict_mode=True),resultado)

    def test_obtener_periodo_incorrecto_strict_mode(self):
        with self.assertRaises(ValueError):
            obtener_periodo(None, None,strict_mode=True)
            obtener_periodo(None, 25,strict_mode=True)
            obtener_periodo(2043, '351',strict_mode=True)
            obtener_periodo(tuple(1,2,3), '351',strict_mode=True)

    def test_obtener_periodo_incorrecto(self):
            self.assertEqual(obtener_periodo(None, None,strict_mode=False),'-1')
            self.assertEqual(obtener_periodo(None, 25,strict_mode=False),'-1')
            self.assertEqual(obtener_periodo(2043, '351',strict_mode=False),'-1')
            self.assertEqual(obtener_periodo({1,2,3}, '351',strict_mode=False),'-1')
            self.assertEqual(obtener_periodo('foo', '351',strict_mode=False),'-1')


    def test_verificar_periodo_parametros_none(self):
        with self.assertRaises(ValueError):
            verificar_periodo(None, None, None,strict_mode=True)

    def test_obtener_trimestre_del_año_correcto(self):
        self.assertEqual(obtener_trimestre_del_año(1,strict_mode=False),1)
        self.assertEqual(obtener_trimestre_del_año(2,strict_mode=False),1)
        self.assertEqual(obtener_trimestre_del_año(3,strict_mode=False),1)

        self.assertEqual(obtener_trimestre_del_año(4,strict_mode=False),2)
        self.assertEqual(obtener_trimestre_del_año(5,strict_mode=False),2)
        self.assertEqual(obtener_trimestre_del_año(6,strict_mode=False),2)

        self.assertEqual(obtener_trimestre_del_año(7,strict_mode=False),3)
        self.assertEqual(obtener_trimestre_del_año(8,strict_mode=False),3)
        self.assertEqual(obtener_trimestre_del_año(9,strict_mode=False),3)

        self.assertEqual(obtener_trimestre_del_año(10,strict_mode=False),4)
        self.assertEqual(obtener_trimestre_del_año(11,strict_mode=False),4)
        self.assertEqual(obtener_trimestre_del_año(12,strict_mode=False),4)

        self.assertEqual(obtener_trimestre_del_año('1',strict_mode=False),1)
        self.assertEqual(obtener_trimestre_del_año('2',strict_mode=False),1)
        self.assertEqual(obtener_trimestre_del_año('3',strict_mode=False),1)

        self.assertEqual(obtener_trimestre_del_año('4',strict_mode=False),2)
        self.assertEqual(obtener_trimestre_del_año('5',strict_mode=False),2)
        self.assertEqual(obtener_trimestre_del_año('6',strict_mode=False),2)

        self.assertEqual(obtener_trimestre_del_año('7',strict_mode=False),3)
        self.assertEqual(obtener_trimestre_del_año('8',strict_mode=False),3)
        self.assertEqual(obtener_trimestre_del_año('9',strict_mode=False),3)

        self.assertEqual(obtener_trimestre_del_año('10',strict_mode=False),4)
        self.assertEqual(obtener_trimestre_del_año('11',strict_mode=False),4)
        self.assertEqual(obtener_trimestre_del_año('12',strict_mode=False),4)

    def test_obtener_trimestre_del_año_parametros_none_strict_mode(self):
        with self.assertRaises(ValueError):
            obtener_trimestre_del_año(None,strict_mode=True)

    def test_obtener_trimestre_del_año_parametros_none(self):
            self.assertEqual(obtener_trimestre_del_año(None,strict_mode=False),-1)

    def test_obtener_trimestre_del_año_mes_fuera_rango_strict_mode(self):
        with self.assertRaises(ValueError):
            obtener_trimestre_del_año(13,strict_mode=True)
            obtener_trimestre_del_año('13',strict_mode=True)
            obtener_trimestre_del_año(0,strict_mode=True)
            obtener_trimestre_del_año('0',strict_mode=True)
            obtener_trimestre_del_año(-13.4,strict_mode=True)
            obtener_trimestre_del_año('A',strict_mode=True)

    def test_obtener_trimestre_del_año_mes_fuera_rango(self):
        self.assertEqual(obtener_trimestre_del_año(13,strict_mode=False),-1)
        self.assertEqual(obtener_trimestre_del_año('13',strict_mode=False),-1)
        self.assertEqual(obtener_trimestre_del_año(0,strict_mode=False),-1)
        self.assertEqual(obtener_trimestre_del_año('0',strict_mode=False),-1)
        self.assertEqual(obtener_trimestre_del_año(-13,strict_mode=False),-1)
        self.assertEqual(obtener_trimestre_del_año('VS',strict_mode=False),-1)
        self.assertEqual(obtener_trimestre_del_año('lechuga',strict_mode=False),-1)

    def test_obtener_mes_tostring_mes_fuera_rango_strict_mode(self):
        with self.assertRaises(ValueError):
            obtener_mes_tostring(None,strict_mode=True)
            obtener_mes_tostring(tuple(125,2),strict_mode=True)
            obtener_mes_tostring(0,strict_mode=True)
            obtener_mes_tostring({-1},strict_mode=True)
            obtener_mes_tostring(-1,strict_mode=True)
            obtener_mes_tostring('-1',strict_mode=True)
            obtener_mes_tostring('ABC',strict_mode=True)
            obtener_mes_tostring('_',strict_mode=True)
            obtener_mes_tostring('1.0',strict_mode=True)
            obtener_mes_tostring(3.14,strict_mode=True)


    def test_obtener_periodo_correcto(self):
        self.assertEqual(obtener_periodo('2021', '12',strict_mode=False),'202112')
        self.assertEqual(obtener_periodo('2024', '11',strict_mode=False),'202411')
        self.assertEqual(obtener_periodo('2017', '10',strict_mode=False),'201710')
        self.assertEqual(obtener_periodo('2017', '7',strict_mode=False),'201707')
        self.assertEqual(obtener_periodo('2017', '07',strict_mode=False),'201707')
        self.assertEqual(obtener_periodo('-1', '10',strict_mode=False),'-1')
        self.assertEqual(obtener_periodo('2042', '-1',strict_mode=False),'-1')
        self.assertEqual(obtener_periodo(2024,10,strict_mode=False),'202410')
        self.assertEqual(obtener_periodo(2024,8,strict_mode=False),'202408')

    def test_obtener_periodo_strict_mode(self):
        with self.assertRaises(ValueError):
            obtener_periodo(2021, None,strict_mode=True)
            obtener_periodo(-1, '11',strict_mode=True)
            obtener_periodo(None, '10',strict_mode=True)

    def test_verificar_periodo_mes_fuera_rango_strict_mode(self):
        with self.assertRaises(ValueError):
            verificar_periodo('2021', '13', '202112',strict_mode=True)
            verificar_periodo('2021', '-1', '202112',strict_mode=True)
            verificar_periodo('2021', None, '202112',strict_mode=True)
            verificar_periodo(None, '-1', '202112',strict_mode=True)
            verificar_periodo(None,None, '202112',strict_mode=True)

    def test_verificar_periodo_mes_fuera_rango(self):
            self.assertFalse(verificar_periodo('2021', '13', '202113',strict_mode=False))
            self.assertFalse(verificar_periodo('-1', '4', '-104',strict_mode=False))
            self.assertFalse(verificar_periodo('2021', '-1', '2021-1',strict_mode=False))
            self.assertFalse(verificar_periodo('2021', None, None,strict_mode=False))
            self.assertFalse(verificar_periodo(None, '-1', None,strict_mode=False))
            self.assertFalse(verificar_periodo(None,None, None,strict_mode=False))

    def test_obtener_num_semana_por_fecha_correcta(self):
        self.assertEqual(obtener_num_semana_por_fecha('25/3/2024',formato='DMA',strict_mode=True),13)
        self.assertEqual(obtener_num_semana_por_fecha('25/03/2024',formato='DMA',strict_mode=True),13)
        self.assertEqual(obtener_num_semana_por_fecha('03/25/2024',formato='MDA',strict_mode=True),13)
        self.assertEqual(obtener_num_semana_por_fecha('3/25/2024',formato='MDA',strict_mode=True),13)
        self.assertEqual(obtener_num_semana_por_fecha('2024/03/25',formato='AMD',strict_mode=True),13)
        #No se puede utilizar formato 'ADM' con este metodo
        self.assertEqual(obtener_num_semana_por_fecha('2024/25/03',formato='ADM',strict_mode=False),'Desconocido')

        self.assertEqual(obtener_num_semana_por_fecha('30/3/2024',formato='DMA',strict_mode=True),13)
        self.assertEqual(obtener_num_semana_por_fecha('31/3/2024',formato='DMA',strict_mode=True),13)

        self.assertEqual(obtener_num_semana_por_fecha('1/4/2024',formato='DMA',strict_mode=True),14)
        self.assertEqual(obtener_num_semana_por_fecha('1/4/2024',formato='DMA',strict_mode=True),14)
        self.assertEqual(obtener_num_semana_por_fecha('1/4/2024',formato='DMA',strict_mode=True),14)
        self.assertEqual(obtener_num_semana_por_fecha('1/4/2024',formato='DMA',strict_mode=True),14)

        self.assertEqual(obtener_num_semana_por_fecha('2/4/2024',formato='DMA',strict_mode=True),14)
        self.assertEqual(obtener_num_semana_por_fecha('5/4/2024',formato='DMA',strict_mode=True),14)
        self.assertEqual(obtener_num_semana_por_fecha('7/4/2024',formato='DMA',strict_mode=True),14)

        self.assertEqual(obtener_num_semana_por_fecha('23/4/2024',formato='DMA',strict_mode=True),17)
        self.assertEqual(obtener_num_semana_por_fecha('29/4/2024',formato='DMA',strict_mode=True),18)
        self.assertEqual(obtener_num_semana_por_fecha('30/4/2024',formato='DMA',strict_mode=True),18)
        self.assertEqual(obtener_num_semana_por_fecha('4/5/2024',formato='DMA',strict_mode=True),18)
        self.assertEqual(obtener_num_semana_por_fecha('3/12/2024',formato='DMA',strict_mode=True),49)
        self.assertEqual(obtener_num_semana_por_fecha('26/12/2024',formato='DMA',strict_mode=True),52)

    def test_obtener_num_dia_semana_por_fecha_correcto(self):
        self.assertEqual(obtener_num_dia_semana_por_fecha('25/3/2024',formato='DMA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('25/03/2024',formato='DMA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/25/2024',formato='MDA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/25/2024',formato='MDA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/25',formato='AMD',strict_mode=True),1)

        self.assertEqual(obtener_num_dia_semana_por_fecha('26/3/2024',formato='DMA',strict_mode=True),2)
        self.assertEqual(obtener_num_dia_semana_por_fecha('26/03/2024',formato='DMA',strict_mode=True),2)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/26/2024',formato='MDA',strict_mode=True),2)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/26/2024',formato='MDA',strict_mode=True),2)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/26',formato='AMD',strict_mode=True),2)

        self.assertEqual(obtener_num_dia_semana_por_fecha('27/3/2024',formato='DMA',strict_mode=True),3)
        self.assertEqual(obtener_num_dia_semana_por_fecha('27/03/2024',formato='DMA',strict_mode=True),3)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/27/2024',formato='MDA',strict_mode=True),3)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/27/2024',formato='MDA',strict_mode=True),3)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/27',formato='AMD',strict_mode=True),3)

        self.assertEqual(obtener_num_dia_semana_por_fecha('28/3/2024',formato='DMA',strict_mode=True),4)
        self.assertEqual(obtener_num_dia_semana_por_fecha('28/03/2024',formato='DMA',strict_mode=True),4)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/28/2024',formato='MDA',strict_mode=True),4)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/28/2024',formato='MDA',strict_mode=True),4)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/28',formato='AMD',strict_mode=True),4)

        self.assertEqual(obtener_num_dia_semana_por_fecha('29/3/2024',formato='DMA',strict_mode=True),5)
        self.assertEqual(obtener_num_dia_semana_por_fecha('29/03/2024',formato='DMA',strict_mode=True),5)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/29/2024',formato='MDA',strict_mode=True),5)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/29/2024',formato='MDA',strict_mode=True),5)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/29',formato='AMD',strict_mode=True),5)

        self.assertEqual(obtener_num_dia_semana_por_fecha('30/3/2024',formato='DMA',strict_mode=True),6)
        self.assertEqual(obtener_num_dia_semana_por_fecha('30/03/2024',formato='DMA',strict_mode=True),6)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/30/2024',formato='MDA',strict_mode=True),6)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/30/2024',formato='MDA',strict_mode=True),6)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/30',formato='AMD',strict_mode=True),6)

        self.assertEqual(obtener_num_dia_semana_por_fecha('31/3/2024',formato='DMA',strict_mode=True),7)
        self.assertEqual(obtener_num_dia_semana_por_fecha('31/03/2024',formato='DMA',strict_mode=True),7)
        self.assertEqual(obtener_num_dia_semana_por_fecha('03/31/2024',formato='MDA',strict_mode=True),7)
        self.assertEqual(obtener_num_dia_semana_por_fecha('3/31/2024',formato='MDA',strict_mode=True),7)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/03/31',formato='AMD',strict_mode=True),7)

        self.assertEqual(obtener_num_dia_semana_por_fecha('1/4/2024',formato='DMA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('1/04/2024',formato='DMA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('04/1/2024',formato='MDA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('4/1/2024',formato='MDA',strict_mode=True),1)
        self.assertEqual(obtener_num_dia_semana_por_fecha('2024/04/1',formato='AMD',strict_mode=True),1)

    def test_obtener_num_dia_semana_por_fecha_parametro_none(self):
        with self.assertRaises(ValueError):
            obtener_num_dia_semana_por_fecha(None,formato='MDA',strict_mode=True)
            obtener_num_dia_semana_por_fecha('04/1/2024',formato=None,strict_mode=True)
            obtener_num_dia_semana_por_fecha(None,formato=None,strict_mode=True)

    def test_obtener_num_dia_semana_por_fecha_error_propuesto_usuario(self):
            self.assertEqual(obtener_num_dia_semana_por_fecha(None,formato='MDA',strict_mode=False,ERROR='Error'),'Error')
            self.assertEqual(obtener_num_dia_semana_por_fecha('04/1/2024',formato=None,strict_mode=False,ERROR='Error'),'Error')
            self.assertEqual(obtener_num_dia_semana_por_fecha(None,formato=None,strict_mode=False,ERROR='Error'),'Error')

    def test_retornar_nombre_dia_correcto(self):
        self.assertEqual(retornar_nombre_dia('1',strict_mode=True),'Lunes')
        self.assertEqual(retornar_nombre_dia('2',strict_mode=True),'Martes')
        self.assertEqual(retornar_nombre_dia('3',strict_mode=True),'Miercoles')
        self.assertEqual(retornar_nombre_dia('4',strict_mode=True),'Jueves')
        self.assertEqual(retornar_nombre_dia('5',strict_mode=True),'Viernes')
        self.assertEqual(retornar_nombre_dia('6',strict_mode=True),'Sabado')
        self.assertEqual(retornar_nombre_dia('7',strict_mode=True),'Domingo')

        self.assertEqual(retornar_nombre_dia(1,strict_mode=True),'Lunes')
        self.assertEqual(retornar_nombre_dia(2,strict_mode=True),'Martes')
        self.assertEqual(retornar_nombre_dia(3,strict_mode=True),'Miercoles')
        self.assertEqual(retornar_nombre_dia(4,strict_mode=True),'Jueves')
        self.assertEqual(retornar_nombre_dia(5,strict_mode=True),'Viernes')
        self.assertEqual(retornar_nombre_dia(6,strict_mode=True),'Sabado')
        self.assertEqual(retornar_nombre_dia(7,strict_mode=True),'Domingo')

        self.assertEqual(retornar_nombre_dia(1.94,strict_mode=True),'Lunes')


    def test_retornar_nombre_dia_parametros_erroneos(self):
        with self.assertRaises(ValueError):
            retornar_nombre_dia(None,strict_mode=True)
            retornar_nombre_dia('foo', strict_mode=True)
            retornar_nombre_dia(-1,strict_mode=True)
            retornar_nombre_dia(0,strict_mode=True)
            retornar_nombre_dia(8,strict_mode=True)


    def test_retornar_nombre_dia_error_propuesto_por_usuario(self):
        self.assertEqual(retornar_nombre_dia(None, ERROR='ERROR', strict_mode=False), 'ERROR')
        self.assertEqual(retornar_nombre_dia('foo', ERROR='ERROR', strict_mode=False), 'ERROR')
        self.assertEqual(retornar_nombre_dia(-1, ERROR='ERROR', strict_mode=False), 'ERROR')


    def test_retornar_nombre_mes_correctos(self):
        self.assertEqual(retornar_nombre_mes(1,strict_mode=True),'Enero')
        self.assertEqual(retornar_nombre_mes(2,strict_mode=True),'Febrero')
        self.assertEqual(retornar_nombre_mes(3,strict_mode=True),'Marzo')
        self.assertEqual(retornar_nombre_mes(4,strict_mode=True),'Abril')
        self.assertEqual(retornar_nombre_mes(5,strict_mode=True),'Mayo')
        self.assertEqual(retornar_nombre_mes(6,strict_mode=True),'Junio')
        self.assertEqual(retornar_nombre_mes(7,strict_mode=True),'Julio')
        self.assertEqual(retornar_nombre_mes(8,strict_mode=True),'Agosto')
        self.assertEqual(retornar_nombre_mes(9,strict_mode=True),'Septiembre')
        self.assertEqual(retornar_nombre_mes(10,strict_mode=True),'Octubre')
        self.assertEqual(retornar_nombre_mes(11,strict_mode=True),'Noviembre')
        self.assertEqual(retornar_nombre_mes(12,strict_mode=True),'Diciembre')

        self.assertEqual(retornar_nombre_mes('1',strict_mode=True),'Enero')
        self.assertEqual(retornar_nombre_mes('2',strict_mode=True),'Febrero')
        self.assertEqual(retornar_nombre_mes('3',strict_mode=True),'Marzo')
        self.assertEqual(retornar_nombre_mes('4',strict_mode=True),'Abril')
        self.assertEqual(retornar_nombre_mes('5',strict_mode=True),'Mayo')
        self.assertEqual(retornar_nombre_mes('6',strict_mode=True),'Junio')
        self.assertEqual(retornar_nombre_mes('7',strict_mode=True),'Julio')
        self.assertEqual(retornar_nombre_mes('8',strict_mode=True),'Agosto')
        self.assertEqual(retornar_nombre_mes('9',strict_mode=True),'Septiembre')
        self.assertEqual(retornar_nombre_mes('10',strict_mode=True),'Octubre')
        self.assertEqual(retornar_nombre_mes('11',strict_mode=True),'Noviembre')
        self.assertEqual(retornar_nombre_mes('12',strict_mode=True),'Diciembre')

    def test_retornar_nombre_mes_parametros_erroneos(self):
        with self.assertRaises(ValueError):
            retornar_nombre_mes(None,strict_mode=True)
            retornar_nombre_mes('lechuga',strict_mode=True)
            retornar_nombre_mes({'lechuga'},strict_mode=True)
            retornar_nombre_mes(13,strict_mode=True)
            retornar_nombre_mes('13',strict_mode=True)
            retornar_nombre_mes('0',strict_mode=True)
            retornar_nombre_mes(0,strict_mode=True)

    def test_retornar_nombre_mes_error_propuesto_por_usuario(self):
            self.assertEqual(retornar_nombre_mes(None,ERROR='Error',strict_mode=False),'Error')
            self.assertEqual(retornar_nombre_mes('lechuga',ERROR='Error',strict_mode=False),'Error')
            self.assertEqual(retornar_nombre_mes({'lechuga'},ERROR='Error',strict_mode=False),'Error')
            self.assertEqual(retornar_nombre_mes(13,ERROR='Error',strict_mode=False),'Error')
            self.assertEqual(retornar_nombre_mes('13',ERROR='Error',strict_mode=False),'Error')
            self.assertEqual(retornar_nombre_mes('0',ERROR='Error',strict_mode=False),'Error')
            self.assertEqual(retornar_nombre_mes(0,ERROR='Error',strict_mode=False),'Error')


if __name__ == '__main__':
    unittest.main()
