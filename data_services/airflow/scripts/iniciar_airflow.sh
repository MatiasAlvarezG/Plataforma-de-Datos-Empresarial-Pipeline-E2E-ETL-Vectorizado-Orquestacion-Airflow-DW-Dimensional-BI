#!/bin/bash
# iniciar_airflow.sh

#################################################################################################################
# Se configura la base de datos y se crean las conexiones para el proyecto.                                     #
# Dado que la base de datos para airflow es en MySQL al momento de desplegar los servicios                      #
# MySQL no puede encargarse de procesar los requerimientos de Apache Airflow (airflow db init)                  #
# ya que se encuentra en un contenedor separado.                                                                #
# Por eso es necesario este script que genera su ejecución luego de haber creado e iniciado todos los servicios.#
# De esta forma se evitará el cierre inesperado a los 3 segundos de Apache Airflow.                             #
# Para el correcto funcionamiento de este script es necesario el archivo de configuración conf.dat.             #
#################################################################################################################

# Directorio del Proyecto
DIR_SCRIPT=$(dirname "$(realpath "$0")")
FILE_CONF=$DIR_SCRIPT/conf.dat

# Se obtiene la ubicación del binario de airflow
BIN_AIRFLOW=$(command -v airflow)

ESTADO_DEL_CONTENEDOR=$(grep ^ESTADO_DEL_CONTENEDOR= $FILE_CONF | cut -d'=' -f2)

# Si no se configuro inicialmente entonces se inicia el proceso

if [[ "$ESTADO_DEL_CONTENEDOR" == "CONFIGURAR" ]]; then
  # Verificar si se encontró el binario
  if [[ -z "$BIN_AIRFLOW" ]]; then
      echo "ERROR ::: El comando 'airflow' no está instalado o no está en el PATH."
      exit 1
  fi

  # Se inicia la base de datos en MySQL para los metadatos de airflow
  $BIN_AIRFLOW db init

  # Se configura el usuario administrador de Airflow
  $BIN_AIRFLOW users create \
      --username airflow \
      --firstname Matias \
      --lastname Alvarez \
      --role Admin \
      --email matias_alvarez_data_engineer@data_engineer.com \
      --password airflow

  # Se crea una conexión para la base de datos SQL Server provista para el proyecto (área de preparación y modelo)
  $BIN_AIRFLOW connections add 'MSSQL_CONN' \
          --conn-type 'mssql' \
          --conn-host $IP_SERVER_SQL \
          --conn-login $DB_USER_PYTHON \
          --conn-password $DB_PWD_PYTHON \
          --conn-schema $DATABASE_SQL \
          --conn-port 1433

  # Se crea una conexión para la base de datos SQL Server provista para la comunicación entre servicios
  $BIN_AIRFLOW connections add 'MSSQL_CONN_COMM' \
          --conn-type 'mssql' \
          --conn-host $IP_SERVER_SQL \
          --conn-login $DB_USER_COM \
          --conn-password $DB_PWD_COM \
          --conn-schema $DATABASE_SQL_COM \
          --conn-port 1433

  # Se crea una conexión para la base de SQL Server provista para verificar las bases de datos existentes
  $BIN_AIRFLOW connections add 'MSSQL_CONN_VER' \
          --conn-type 'mssql' \
          --conn-host $IP_SERVER_SQL \
          --conn-login $DB_USER_VER \
          --conn-password $DB_PWD_VER \
          --conn-schema 'master' \
          --conn-port 1433

  # Se actualiza el estado del archivo conf.dat
  sed -i 's/^ESTADO_DEL_CONTENEDOR\s*=\s*.*/ESTADO_DEL_CONTENEDOR=INICIAR_AIRFLOW/' $FILE_CONF
  ESTADO_DEL_CONTENEDOR=INICIAR_AIRFLOW
fi

if [[ "$ESTADO_DEL_CONTENEDOR" == "INICIAR_AIRFLOW" ]]; then
  # Para evitar problemas se cierra cualquier proceso que haya quedado abierto
  kill $(pgrep airflow)

  # Se inicia el proceso de inicio de airflow
  $BIN_AIRFLOW scheduler & $BIN_AIRFLOW webserver
fi

# Se establece que el script quede abierto
tail -f /dev/null


