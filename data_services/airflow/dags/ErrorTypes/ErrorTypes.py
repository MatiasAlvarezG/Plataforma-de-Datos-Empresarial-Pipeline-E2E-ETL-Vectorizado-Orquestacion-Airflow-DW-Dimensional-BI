class EventFailedError(Exception):
    '''Clase de error que define falla en la ejecución o resolución de un evento.'''
    def __init__(self, *args):
        super().__init__(*args)

class UndefinedEnvironmentVariable(Exception):
    '''Clase de error que define falla de definición de variables de entornos necesarias.'''
    def __init__(self, *args):
        super().__init__(*args)
