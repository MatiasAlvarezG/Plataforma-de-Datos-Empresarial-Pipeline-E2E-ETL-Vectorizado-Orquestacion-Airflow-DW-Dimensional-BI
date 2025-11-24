# Métodos relacionados a las fechas
from re import compile
from pandas import to_datetime

def __validar_mes__(mes):
    '''Retorna True si mes esta comprendido en un valor entre 1 y 12, caso contrario retorna False.'''
    try:
        mes = int(mes) if not isinstance(mes, int) else mes
        return not (mes < 1 or mes > 12)
    except:
        return False


def validar_fecha(fecha : str, 
                  formato : str ='MDA',
                  strict_mode : bool = False) -> bool:
    '''
    Valida que la fecha pasada por parámetro es valida y cumple con el formato mencionado.
    El cliente debe pasar una fecha que garantice separadores como los siguiente: ('/','-','') 
    entre las fechas.

    Ejemplo de fechas validas:
    >>> '12/5/2024' , formato='DMA'
    >>> '12-5-2024' , formato='DMA'
    >>> '1252024'  ,  formato='DMA'

    Ejemplo de fechas nos validas:
    >>> '12$5$2024' , formato='DMA'
    >>> '12/5/2024' , formato='DMA'


    '''
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if fecha is None:
        if not strict_mode:
            return False
        raise ValueError('ERROR ::: La fecha no puede ser NoneType.')
    
    if formato is None:
        if not strict_mode:
            return False
        raise ValueError('ERROR ::: El formato no puede ser NoneType.')  

    return extraer_partes_fecha(fecha,formato)[0] != '-1'


def _verificar_formato(formato: str, 
                        formato_correcto: list = ['MDA', 'DMA', 'AMD', 'ADM'], 
                        strict_mode: bool = True, 
                        ERROR: str = ('-1', '-1', '-1')) -> str|tuple:
    '''
    Retorna un formato estandarizado si el mismo es correcto.
    Esto quiere decir que si se pasa un formato 'adm' el método retornara 'ADM'.
    Caso contrario retornara una excepción o un error definido por el usuario.

    Parameters
    ----------
    - formato : str
        El formato de fecha a verificar y normalizar.
    - formato_correcto : list
        Lista de formatos de fecha válidos. Por defecto, ['MDA', 'DMA', 'AMD', 'ADM'].
    - strict_mode : bool
        Si es True, se genera una excepción si el formato no es válido. 
        Si es False, se devuelve el valor de ERROR. 
        Por defecto es True.
    - ERROR : str or tuple
        Valor a devolver en caso de que el formato no sea válido. 
        Por defecto retornal la tupla: ('-1', '-1', '-1').

    Returns
    -------
    str
        El formato de fecha normalizado si es válido, o el valor de ERROR si strict_mode es False.

    Raises
    ------
    ValueError
        Si el formato no es válido y strict_mode es True.

    Examples
    --------
    >>> _verificar_formato_fecha(formato='AMD', formato_correcto=['MDA', 'DMA', 'AMD', 'ADM']) 
        Retorna: 'AMD'

    >>> _verificar_formato_fecha(formato='aMd', formato_correcto=['MDA', 'DMA', 'AMD', 'ADM']) 
        Retorna: 'AMD'
    
    >>> _verificar_formato_fecha(formato='AñoMeSdIA', formato_correcto=['MDA', 'DMA', 'AMD', 'ADM'], strict_mode=True) 
        Genera: ValueError

    >>> _verificar_formato_fecha(formato='AñoMeSdIA', formato_correcto=['MDA', 'DMA', 'AMD', 'ADM'], strict_mode=False) 
        Retorna: ('-1', '-1', '-1')

    >>> _verificar_formato_fecha(formato='A', formato_correcto=['MDA', 'DMA', 'AMD', 'ADM'], strict_mode=False, ERROR='-1') 
        Retorna: '-1'
    '''
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if formato is None:
        if strict_mode:
            raise ValueError('ERROR ::: Formato no tiene referencia')
        return ERROR
    
    if not isinstance(formato, str):
        if strict_mode:
            raise TypeError(f"ERROR ::: El formato debe ser de tipo str y debe tener alguno de los siguientes valores: {formato_correcto}.")
        return ERROR
    
    if formato.upper() not in formato_correcto:
        if strict_mode:
            raise ValueError(f"ERROR ::: Formato erróneo. El formato debe ser alguno de los siguientes: {formato_correcto}.")
        return ERROR
    
    return formato.upper() if ( not formato.isupper()) else formato


def _extraer_partes_fecha_fila(fila, 
                               columnas: list = ['Fecha_Año', 'Fecha_Mes', 'Fecha_Dia']) -> tuple:
    '''
    Retorna el año, mes y día de una fila de un DataFrame de pandas o una estructura similar.
    
    Parameters
    ----------
    fila : pandas.Series
        La fila de un DataFrame de pandas o una estructura similar que contiene la fecha.
    columnas : list, optional
        Los nombres de las columnas que representan el año, mes y día, respectivamente.
        Deben estar en el orden 'AMD' (Año, Mes, Día). Por defecto, ['Fecha_Año', 'Fecha_Mes', 'Fecha_Dia'].
    
    Returns
    -------
    tuple
        Una tupla que contiene el año, mes y día de la fecha en la fila.

    Raises
    ------
    KeyError
        Si no se encuentran las columnas necesarias para extraer la fecha.
    IndexError
        Si no se proporcionan suficientes columnas para extraer la fecha.

    Example
    -------
    >>> _verificar_partes_fecha(fila, columnas=['Año', 'Mes', 'Día'])
        Retorna: (2024, 6, 12)
    '''
    if fila is None:
        raise ValueError('ERROR ::: No puede pasar una fila de tipo NoneType.')
    try:
        año = fila[columnas[0]]  
        mes = fila[columnas[1]]
        dia = fila[columnas[2]] 
        if validar_fecha_entera_partes(dia=dia,mes=mes,año=año):
                return (año, mes, dia) 
        raise ValueError('ERROR ::: No se pudo extraer las partes correspondiente de la fecha.')

    except KeyError as e:
        raise KeyError('ERROR ::: Columnas Erróneas al intentar conseguir los valores de Fecha, verifique las columnas pasadas del dataframe, RECUERDE: DEBE TENER EL FORMATO AA/MM/DD')
    except IndexError as e:
        raise IndexError('ERROR ::: No se pasaron las columnas suficientes para conseguir la extracción de fecha correspondiente.')


def es_bisiesto(año : int, strict_mode : bool = False) -> bool:
    '''
    Retorna True si un año es bisiesto caso contrario retorna False.
    El año debe ser mayor o igual a 0.
    '''
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if año is None:
        if not strict_mode:
            return False
        raise ValueError('ERROR ::: Debe pasar una entero que represente un año y no un NoneType.')
    
    if not isinstance(año, int):
        try:
            año = int(año)
        except:
            if not strict_mode:
                return False
            raise ValueError(f'ERROR ::: debe pasar un entero que represente un año.')

    if año < 0:
        if not strict_mode:
            return False
        raise ValueError('ERROR ::: Debe pasarse un año positivo.')
    
    return año % 4 == 0 and (año % 100 != 0 or año % 400 == 0)


def validar_fecha_entera_partes(dia,
                                mes,
                                año,
                                strict_mode : bool = False)  ->  bool:
    '''
    Valida que un dia, mes y año cumpla con los rangos establecidos:
        dias(28 a 31), mes(1 a 12) y año (con un maximo de 2050)

    Example
    -----
    >>> validar_fecha_entera_parte(12,6,2024)
        retornara: True

    >>> validar_fecha_entera_parte(31,6,2024)
        retornara: False (Junio contiene 30 días como máximo)

    >>> validar_fecha_entera_parte(1,16,2024)
        retornara: False

    >>> validar_fecha_entera_parte(29,2,2023)
        retornara: False (En 2023 febrero no tiene 29 días)

    >>> validar_fecha_entera_parte(29,2,2024)
        retornara: True (En 2024 febrero tiene 29 días)
    '''
    
    dias_por_mes = {
        '1' : 31, #Enero
        '2' : 28, #Febrero - 29 días años bisiestos
        '3' : 31, #Marzo
        '4' : 30, #Abril
        '5' : 31, #Mayo
        '6' : 30, #Junio
        '7' : 31, #Julio
        '8' : 31, #Agosto
        '9' : 30, #Septiembre
        '10': 31, #Octubre
        '11': 30, #Noviembre
        '12': 31  #Diciembre
    }
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if None in [dia,mes,año]:
        if strict_mode:
            raise ValueError('ERROR ::: Se paso algún componente de Fecha como NoneType.')
        return False
    try:
        dia = int( dia ) if not isinstance( dia, int ) else dia
        mes = str( mes ) if not isinstance( mes, str ) else mes # str para poder usarlo como clave
        año = int( año ) if not isinstance( año, int ) else año
        # Para manejar los casos donde mes es un string con un cero antepuesto. Por ejemplo: '02'
        mes = mes[1] if (len(mes)==2  and mes[0]=='0' and __validar_mes__(mes)) else mes

        max_dias = dias_por_mes[mes] if not ( mes=='2' and es_bisiesto(año) ) else 29

        return ( dia>0 and dia<=max_dias ) and ( año>=1900 and año<=2050 )
    
    # Mes incorrecto o los datos son incorrectos
    except Exception as e:
        if strict_mode:
            ERROR = type(e)
            raise ERROR('ERROR ::: No se pudo validar la fecha.\nerror: {e}')
        return False
    

def extraer_partes_fecha(fecha: str, formato: str = 'MDA', strict_mode: bool = False) -> tuple:
    '''
    Extrae los componentes de una fecha de una cadena y los devuelve como una tupla que sigue el formato 'DMA'.
    Formato Validos de fechas a recibir: 'MDA' (MM/DD/YYYY), 'DMA' (DD/MM/AA) , 'AMD' (AA/MM/DD), 'ADM' (AA/DD/MM) .\n
    NOTA IMPORTANTE: 
        Es recomendable que utilice separadores para evitar problemas de detección de grupos
        También se recomienda anteponer un '0' en los meses comprendidos entre marzo y septiembre, puede logarlo
        utilizando el método obtener_mes_tostring(mes,strict_mode=False).
    Parameters
    ----------
    - fecha : str
        La cadena que representa la fecha.
    - formato : str
        El formato de la variable fecha. Puede ser 'MDA' (MM/DD/YYYY), 'DMA' (DD/MM/YYYY), 
        'AMD' (YYYY/MM/DD) o 'ADM' (YYYY/DD/MM). Por defecto, 'MDA'.
    - strict_mode : bool
        Si es True, se genera una excepción si la fecha no coincide con el formato especificado. 
        Si es False, devuelve la tupla: ('-1', '-1', '-1') en caso de error. Por defecto, False.

    Returns
    -------
    tuple
        Una tupla que contiene el día, mes y año de la fecha (formato: 'DMA').
        En caso de falla y strict_mode sea False entonces se retorna ('-1','-1','-1').

    Examples
    --------
    >>> extraer_partes_fecha('12/6/2024', formato='DMA')
        Retorna: (12, 6, 2024)

    >>> extraer_partes_fecha('12/6/2024', formato='MDA')
        Retorna: (6, 12, 2024)

    >>> extraer_partes_fecha('2024/12/6', formato='AMD')
        Retorna: (6, 12, 2024)

    >>> extraer_partes_fecha('2024/12/6', formato='ADM')
        Retorna: (12, 6, 2024)

    >>> extraer_partes_fecha('4/12/6', formato='ADM')
        Retorna: ('-1', '-1', '-1')
    '''

    # Nota: en caso de modificar ERROR tener en cuenta que se utiliza como convención en algunos métodos (validar_fecha())
    ERROR = ( '-1', '-1', '-1' )#'-1' #'Desconocido'

    if fecha is None:
        if strict_mode:
            raise ValueError('ERROR ::: fecha no puede ser NoneType')    
        return ERROR
    
    formato_correcto = ['MDA','DMA','AMD','ADM']
    formato = _verificar_formato(formato, 
                                formato_correcto, 
                                strict_mode=strict_mode, 
                                ERROR=ERROR)    

    if formato==ERROR:
        if strict_mode:
            raise ValueError(f'ERROR ::: El formato pasado no corresponde a una fecha valida. \
                            el formato debe ser del tipo {formato_correcto}')
        return ERROR 
    
    # Expresión regular para fechas en formato MM/DD/YYYY ó DD/MM/YYYY
    if (formato in ['MDA','DMA']):
        regex =  compile(r'^(\d{1,2})[/-]?(\d{1,2})[/-]?(\d{4})$') 
        grupo_dia, grupo_mes, grupo_año = (2,1,3) if formato in 'MDA' else (1,2,3)
    else: ## AA/MM/DD ó AA/DD/MM
        regex =  compile(r'^(\d{4})[/-]?(\d{1,2})[/-]?(\d{1,2})$') 
        grupo_dia, grupo_mes, grupo_año = (3,2,1) if formato in 'AMD' else (2,3,1)

    if None in [regex, grupo_año, grupo_dia, grupo_mes]:
        if strict_mode:
            raise ValueError('ERROR ::: Hubo un error al procesar la fecha')
        return ERROR
    

    # Se intenta hacer coincidir la expresión regular con la fecha
    coincidencia = regex.match( fecha )
    if coincidencia:
        # En caso afirmativo se extrae los componentes de la fecha
        dia = int( coincidencia.group(grupo_dia) )
        mes = int( coincidencia.group(grupo_mes) ) 
        año = int( coincidencia.group(grupo_año) )
    else: 
        if strict_mode:
            raise ValueError('ERROR ::: No se pudo conseguir los grupos de fechas.')
        return ERROR
    return ( dia, mes, año ) \
        if (coincidencia and
            validar_fecha_entera_partes(dia, 
                                        mes, 
                                        año,
                                        strict_mode=False)) \
        else ERROR 


def transformar_fecha_por_formato_por_fila(fila, 
                                        formato='MDA', 
                                        sep='/',
                                        columnas=['Fecha_Año', 'Fecha_Mes', 'Fecha_Dia'],
                                        strict_mode=True) -> str:
    '''
    Corrige el formato de fecha de un registro en una Serie o estructura similar,
    utilizando un separador definido por el usuario.
    
    Con este método se logra modificar e
    Parámetros:
    -----------
        fila: Serie o estructura similar
            El registro que se desea corregir.
        
        formato: str
            El formato deseado para la fecha. Puede ser 'MDA', 'DMA', 'AMD' o 'ADM'.
            Por defecto su valor es 'MDA'.
        sep: str
            El separador utilizado para separar los elementos al retornar la cadena
            Por defecto su valor es '/'
        columnas: list
            Los nombres de las columnas correspondientes al año, mes y día, 
            en el orden adecuado('AMD')para el formato de fecha especificado.
            Por defecto su valor es: ['Fecha_Año', 'Fecha_Mes', 'Fecha_Dia'].
    
    Retorna:
    --------
        str: La fecha corregida en el formato especificado.
    '''
    if not isinstance(strict_mode,bool):
        strict_mode=True

    if fila is None:
        raise ValueError('ERROR ::: El parámetro fila no puede ser de tipo NoneType')
    
    try:
        formato = _verificar_formato(formato, strict_mode=strict_mode) 
        año, mes, dia = _extraer_partes_fecha_fila(fila,columnas)
    except ValueError as e:
        raise ValueError(f'ERROR ::: Problemas al verificar el formato {e}')
    except (KeyError,IndexError) as e:
        raise type(e)(f'ERROR ::: Problemas al verificar las partes de la fecha {e}')

    if (formato in ['MDA','DMA']):
        return f'{mes}{sep}{dia}{sep}{año}' if formato == 'MDA' else  \
               f'{dia}{sep}{mes}{sep}{año}'

    else: 
        return f'{año}{sep}{mes}{sep}{dia}' if formato=='AMD' else \
               f'{año}{sep}{dia}{sep}{mes}'

#@Desprecated
def fecha_DMA_to_MDA(dia,mes,año,simb='/'):
    '''
    Convertir una fecha con formato DMA a MDA separador por un separador definido por el usuario.
    '''
    return f'{mes}{simb}{dia}{simb}{año}'


def obtener_id_fecha_fila(fila, 
                         formato : str = 'MDA', 
                         columnas = ['Fecha_Año','Fecha_Mes','Fecha_Dia']) -> str: 
    '''
    Crea un IdFecha desde una fila de una Serie basado en las columnas de fecha establecidas en el parámetro
    columnas, recordar que el formato de las columnas debe ser 'AMD'.
    El id esta conformado por el formato pasado por parámetro.

    Example
    -----
    >>> obtener_id_fecha_fila({'Fecha_Año': 2022, 'Fecha_Mes': 6, 'Fecha_Dia': 12},'MDA')
        Retornara: '6122022'
    '''
    if fila is None:
        raise ValueError('ERROR ::: El parámetro fila no puede ser de tipo NoneType.')
    
    if formato is None:
        raise ValueError('ERROR ::: El parámetro formato no puede ser de tipo NoneType.')
    
    try:
        formato = _verificar_formato(formato, strict_mode=True) 
        año, mes, dia = _extraer_partes_fecha_fila(fila,columnas)
        
    except ValueError as e:
        raise ValueError(f'ERROR ::: Problemas al verificar el formato {e}')
    except (KeyError,IndexError) as e:
        raise type(e)(f'ERROR ::: Problemas al verificar las partes de la fecha {e}')

    if 0 < int(dia) < 10:
        dia = f'0{int(dia)}'
    
    if columnas[1] not in ['Fecha_Mes_Str','Fecha_Mes_Entrega_Str', 'Fecha_Mes_Venta_Str'] and 0 < int(mes) < 10:
        mes = f'0{int(mes)}'
        
    if (formato in ['MDA','DMA']):
        return f'{mes}{dia}{año}' if formato == 'MDA' else \
               f'{dia}{mes}{año}'
    else: 
        return f'{año}{mes}{dia}' if formato == 'AMD' else \
               f'{año}{dia}{mes}'


def obtener_mes_tostring(mes : int|str, strict_mode : bool = False) -> str:
    '''
    Retorna el string del mes anteponiendo un 0 si se encuentre en el rango Enero-Septiembre.
    Puede recibir el parámetro mes como tipo cadena o entera.
    si strict_mode es False retornara -1 en caso de falla, caso contrario lanzara una excepción
    '''
    if not isinstance(strict_mode,bool):
        strict_mode=True

    if mes is None:
        if strict_mode:
            raise ValueError('ERROR ::: El parámetro mes no puede ser de tipo NoneType.')
        return '-1'
    
    if not isinstance(mes,int):
        try:
            mes = int(mes)
        except:
            if strict_mode:
                raise ValueError('ERROR ::: El parámetro mes no es de un tipo (int) valido en el rango de [1;12].')
            return '-1'
        
    if not __validar_mes__(mes):
        if strict_mode:
            raise ValueError('ERROR ::: El parámetro mes no esta en el rango de valores [1;12].')
        return '-1'
    
    return f'{mes}' if int(mes)>9 else (f'0{mes}')


def obtener_periodo(año : str, mes : str, strict_mode : bool = False) -> str:
    '''
    Retorna el string del periodo del año. Formato: AAAAMM. 
    Si mes esta entre Enero y Septiembre se antepone un 0.
    Si strict_mode es False retornara -1 en caso de fallos, si no, lanzara una excepción.
    Es responsabilidad del cliente pasar un año y mes valido.
    '''
    if not isinstance(strict_mode,bool):
        strict_mode=True

    if mes is None or año is None:
        if strict_mode:
            raise ValueError('ERROR ::: El mes o el Año no pueden ser de tipo NoneType.')
        return '-1'
    
    try:
        mes = str(mes)
        año = str(año)
    except:
        if strict_mode:
            raise TypeError(f'ERROR ::: Se esperaba que mes y año fueran de tipo String.')
        return '-1'
    
    if (mes=='-1' or año=='-1'):
        if strict_mode:
            raise ValueError(f'ERROR ::: Fecha recibida no valida: mes: {mes} año: {año}')
        return '-1'
    
    mes = obtener_mes_tostring(mes)
    if mes=='-1':
        if strict_mode:
            raise ValueError('El mes esta fuera de rango [1;12].')
        return '-1'
    
    return f'{año}{mes}'


def verificar_periodo(año,
                      mes : str, 
                      periodo, 
                      strict_mode:bool = False) -> bool:
    '''
    Verifica si dado un año y un mes este es igual a un periodo.
    Útil para verificar si un periodo provisto es correcto basado en un año y mes correcto
    IMPORTANTE: Dependiendo de la lógica del periodo provista puede que deba anteponer un '0' 
    a su mes si este esta comprendido entre Marzo y Septiembre

    Example
    -----
    >>> verificar_periodo('2024','06','20246')
        retornara: True
    '''
    if not isinstance(strict_mode,bool):
        strict_mode=True

    if mes is None or año is None or periodo is None:
        if strict_mode:
            raise ValueError('ERROR ::: El mes o el Año no pueden ser de tipo NoneType.')
        return False
    
    if not __validar_mes__(mes):
        return False
    
    return f'{año}{mes}'== f'{periodo}'


def obtener_trimestre_del_año(mes : int, strict_mode : bool = False) -> int:
    '''
    Indica el número de trimestre del año según el mes\n
    El parámetro strict_mode en True retornara una excepción en caso de alguna falla
    si esta en False entonces retornara -1 en vez de una excepción
    '''
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if mes is None:
        if not strict_mode:
            return -1
        raise ValueError('ERROR ::: El parámetro mes no contiene referencia')


    if not isinstance(mes, int):
        try:
            mes = int(mes)
        except ValueError as e:
            if not strict_mode:
                return -1
            raise ValueError('ERROR ::: Debe pasar un valor entero entre 1 y 12 inclusive.')
        
    if not __validar_mes__(mes):
        if strict_mode:
            raise ValueError('ERROR ::: Mes debe ser un valor entre 1 y 12 inclusive.')
        return -1
    
    return 1 if mes in [1, 2, 3] else \
           (2 if mes in [4, 5, 6] else \
           (3 if mes in [7, 8, 9] else \
           (4 if mes in [10,11,12] else -1))
           )


def  obtener_num_semana_por_fecha(fecha : str, 
                                 formato : str,
                                 ignorar_fecha = '-1',
                                 ERROR : str = 'Desconocido', 
                                 strict_mode : bool = False) -> str:
    '''
    Retorna el numero correspondiente de la semana del año. Tiene un valor de 1 a 52
    El parámetro ERROR es el valor a retornar si no se puede obtener la semana del año y además strict_mode es False
    El parámetro ignorar_fecha hace alusión a las fechas que no se pueden procesar porque son Desconocidas, estas
    fechas generalmente están referenciada por el valor '-1' o 'Desconocido'
    El formato debe ser pasado correctamente o no se podrá retornar la fecha correcta
    IMPORTANTE: FORMATO NO VALIDO : 'ADM'
    '''
    FORMATO_NO_VALIDO = ['ADM']
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if ERROR is None:
        ERROR = 'Desconocido'

    if fecha is None:
        if strict_mode:
            raise ValueError('ERROR ::: El parámetro fecha no puede ser de tipo NoneType.')
        return ERROR
    
    if formato is None:
        if strict_mode:
            raise ValueError('ERROR ::: El parámetro formato no puede NoneType.')
        return ERROR
    
    if formato in FORMATO_NO_VALIDO:
        if strict_mode:
            raise ValueError(f'ERROR ::: El parámetro formato no puede ser {formato}.')
        return ERROR      
    
    try:
        formato = _verificar_formato(formato,strict_mode=strict_mode,ERROR=ERROR)
    except ValueError as e:
        if strict_mode:
            raise e
        return ERROR
    
    if not isinstance(fecha,str):
        try:
            fecha = str(fecha)
        except ValueError:
            if strict_mode:
                raise ValueError('ERROR ::: Se esperaba un string como fecha.')
            return ERROR
        
    format = '%d/%m/%Y' if formato in ['DMA'] \
                        else ('%m/%d/%Y') if formato in ['MDA'] else 'ISO8601' #\
                            #  ('/%Y/%d/%m' if formato in ['ADM'] else 'ISO8601' )
    try:
        return to_datetime(fecha,
                           format=format)\
                .isocalendar().week  if fecha!=ignorar_fecha else ERROR
    except Exception as e:
        if strict_mode:
            raise e
        return ERROR


def obtener_num_dia_semana_por_fecha(fecha : str, 
                              formato : str, 
                              ignorar_fecha = '-1',
                              ERROR : str = 'Desconocido', 
                              strict_mode : bool = False) -> str:  
    '''
    Retorna el número correspondiente al día de la semana.
    'Lunes' seria 1, 'Martes' seria 2,..., 'Domingo' seria 7 
    '''    
    if not isinstance(strict_mode,bool): 
        strict_mode = True

    if ERROR is None:
        ERROR = 'Desconocido'

    if fecha is None:
        if strict_mode:
            raise ValueError('ERROR ::: La fecha no puede ser NoneType.')
        return ERROR
    
    if not isinstance(fecha, str):
        try:
            fecha = str(fecha) 
            dia,mes,año = extraer_partes_fecha(fecha=fecha,formato=formato)
            if not validar_fecha(dia=dia,mes=mes,año=año):
                if strict_mode:
                    raise ValueError(f'La fecha no es valida para calcular el día de la semana.')
                return ERROR
        except ValueError as e:
            if strict_mode:
                raise e
            return ERROR   

    try:
        formato = _verificar_formato(formato,strict_mode=strict_mode,ERROR=ERROR)
    except ValueError as e:
        if strict_mode:
            raise e
        return ERROR
    
    format = '%d/%m/%Y' if formato in ['DMA'] \
                    else ('%m/%d/%Y') if formato in ['MDA'] else 'ISO8601' #\
                        #  ('/%Y/%d/%m' if formato in ['ADM'] else 'ISO8601' )
    try:
        return to_datetime(fecha,
                           format=format)\
                .isocalendar().weekday if fecha!=ignorar_fecha else ERROR
    except Exception as e:
        if strict_mode:
            raise e
        return ERROR


def retornar_nombre_dia(dia:int, 
                        ERROR = 'Desconocido',
                        strict_mode : bool = False) -> str:
    '''
    Retorna el nombre del día.

    Parameters
    -----
        - dia : int
            Entero entre 1 y 7 donde sigue el sentido normal de los días, es decir, 1 es 'Lunes', 
            2 es 'Martes',...,6 es 'Sabado' y 7 es 'Domingo'\n
        
        - strict_mode : bool
            Si es True hace que cuando haya alguna falla lógica o un error retorne una excepción.
            Si es False hace que cuando haya alguna falla lógica o un error se retorne 'Desconocido'.

    Example
    ----
    >>> retornar_nombre_dia(5) retornara : Viernes
    >>> retornar_nombre_dia(-1,True) retornara : ValueError
    >>> retornar_nombre_dia(0,False) retornara : 'Desconocido'

    '''
    if not isinstance(strict_mode,bool):
        strict_mode = True

    if dia is None:
        if not strict_mode:
            return ERROR
        raise ValueError('ERROR ::: El parámetro día no contiene referencia.')
    
    if not isinstance(dia, int):
        try:
            dia = int(dia)
        except ValueError as e:
            if not strict_mode:
                return ERROR
            raise ValueError('ERROR ::: Debe pasar un valor entero entre 1 y 7 inclusive.')
    
    if 1 < dia > 7:
        if strict_mode:
            raise ValueError('ERROR ::: Dia debe ser un valor entre 1 y 7 inclusive.')
        return ERROR
    
    dias_semana = {
        1 : 'Lunes',
        2 : 'Martes',
        3 : 'Miercoles',
        4 : 'Jueves',
        5 : 'Viernes',
        6 : 'Sabado',
        7 : 'Domingo',
    }
    return dias_semana[dia] if (0 < dia < 8) else ERROR


def retornar_nombre_mes(mes : int, 
                        ERROR = 'Desconocido', 
                        strict_mode : bool = False) -> str:
    '''
    Retorna el nombre del mes según el número del mismo.\n
    1 sería 'Enero', 2 sería 'Febrero',...,12 sería 'Diciembre'.
    '''
    if not isinstance(strict_mode,bool):
        strict_mode = True
    
    if mes is None:
        if strict_mode:
            raise ValueError('ERROR ::: El mes no puede ser de tipo NoneType.')
        return ERROR
    
    if not isinstance(mes,int):
        try: 
            mes = int(mes)
        except Exception as e: 
            if strict_mode:
                raise e
            return ERROR
        
    mes_nombre = {
        1 : 'Enero',
        2 : 'Febrero',
        3 : 'Marzo',
        4 : 'Abril',
        5 : 'Mayo',
        6 : 'Junio',
        7 : 'Julio',
        8 : 'Agosto',
        9 : 'Septiembre',
        10 : 'Octubre',
        11 : 'Noviembre',
        12 : 'Diciembre',
    }
    return mes_nombre[mes] if __validar_mes__(mes) else ERROR














































# Matias Alvarez - Data Engineer
