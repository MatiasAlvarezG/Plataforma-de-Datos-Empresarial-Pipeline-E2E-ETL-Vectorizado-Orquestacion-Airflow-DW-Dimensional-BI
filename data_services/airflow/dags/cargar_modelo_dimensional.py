from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from ErrorTypes.ErrorTypes import EventFailedError
from utils import utils


@task(show_return_value_in_logs=True)
def _cargar_datos_procesados():
    from time import sleep
    RESULTADO = utils._enviar_mensaje_mssql('iniciar_modelo')

    if RESULTADO not in ('ALREADY DONE', 'OK'):
        raise EventFailedError(f'ERROR ::: La solicitud de carga del modelo dimensional falló. El resultado obtenido fue: {RESULTADO}')

    sleep(30)
    return 'end_task'

# Se crea el DAG
with DAG(
    dag_id='carga_modelo_dimensional',
    default_args=utils.default_args,
    description='DAG especifico para carga los datos procesados del área de preparación de forma manual.',
    schedule=None,
    catchup=False,
    tags=['CARGA', 'MODELO-DIMENSIONAL', 'MMSQL'],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    load_dimensional_model_task = _cargar_datos_procesados()

    end_task = EmptyOperator(
        task_id='end_task',
        trigger_rule='all_done'
    )

    ###################
    # Ejecución Tareas
    ###################
    start_task >> load_dimensional_model_task >> end_task
    
