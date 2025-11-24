# Métodos utilizados en DataFrames de tipo proveedor
from pandas import Series


def encontrar_tipo_sociedad(serie : Series, not_found = 'Desconocido'):
    '''
    Función que retorna (en mayúscula) el tipo de sociedad de una empresa. 
    Puede definirse el tipo de retorno al no encontrar una sociedad con la variable ´not_found´

    Notes
    -------
    Para funcionar correctamente el tipo de sociedad debe estar en la ultima parte de la oración.
    Además recordar que todos que el formato aceptado es Title para todas las cadenas de texto

    Example
    --------
    >>> encontrar_tipo_sociedad(Series(['Empresa Sa','Empresa Srl', 'Empresa X']))
        Retornara: Series(['SA', 'SRL', 'Desconocido'])
    '''
    if serie is None:
        raise ValueError('ERROR ::: Se esperaba que el parámetro series fuera una Series de pandas y no un NoneType')

    if not isinstance(serie, Series):
        raise TypeError('ERROR ::: Se esperaba que el parámetro series fuera una Series de pandas')
    sociedades = ['Sa','Srl','Sas','Srlu','Scs','Sca','Sc','Cooperativa']
    return serie.replace('[^A-Za-z0-9\s]', '', regex=True) \
                .str.title() \
                .str.split(' ') \
                .apply(lambda x : x[-1].upper() if x[-1] in sociedades else not_found)

















# Matias Alvarez - Data Engineer