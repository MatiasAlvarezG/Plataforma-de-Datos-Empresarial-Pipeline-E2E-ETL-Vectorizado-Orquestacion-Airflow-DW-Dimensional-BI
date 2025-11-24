from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import State
from datetime import timedelta
from ErrorTypes.ErrorTypes import EventFailedError
from utils import utils


@task(show_return_value_in_logs=True)
def _procesar_y_cargar():
    from time import sleep

    RESULTADO = utils._enviar_mensaje_mssql('procesar_y_iniciar_modelo')
    if RESULTADO not in ('ALREADY DONE', 'OK'):
        raise EventFailedError(f'ERROR ::: La solicitud de procesamiento y carga falló. El Resultado es: {RESULTADO}')

    sleep(100)
    return 'end_task'


with DAG(
    dag_id='procesamiento_area_preparacion_y_carga_modelo_dimensional',
    default_args=utils.default_args,
    description='Se procesa el área de preparación y se carga al modelo dimensional. Se necesita que se haya ejecutado el DAG con id: procesamiento_y_carga_all_dataset',
    schedule=None,
    catchup=False,
    tags=['PROCESAMIENTO', 'AREA-PREPARACION','CARGA-MODELO-DIMENSIONAL', 'MSSQL'],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    processing_load_task = _procesar_y_cargar()

    end_task = EmptyOperator(
        task_id='end_task',
        trigger_rule='all_done'
    )

    ###################
    # Ejecución Tareas
    ###################
    ## start_task >> wait_for_etl >> processing_load_task >> end_task
    start_task >> processing_load_task >> end_task
    