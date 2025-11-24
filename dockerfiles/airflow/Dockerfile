# Se instala la imagen de Python 3.11
FROM python:3.11-slim

### Se define las variables de entorno para el proyecto 

### [BASE DE DATOS] ###
ENV IP_SERVER_SQL=sql_server
ENV DATABASE_SQL=proyecto_matias_alvarez
ENV DB_USER_PYTHON=python
ENV DB_PWD_PYTHON=Pyth0N_p4ssw0rd.

# Se define las variables de entorno para comunicarse con MSSQL 
ENV DATABASE_SQL_COM='comunication_db'
ENV DB_USER_COM='com_user'
ENV DB_PWD_COM='C0m_p4ssw0rd.'

# Se define las variables de entornos para verificar las base de datos
ENV DATABASE_SQL_VER=master
ENV DB_USER_VER=user_verificacion_db
ENV DB_PWD_VER=User_V3r1ficaci0n.

# Se define las variables de entorno para MySQL [MetaData]
ENV DATABASE_MYSQL_METADATA=airflow_db
ENV DB_USER_METADATA=airflow_user
ENV DB_PWD_METADATA=Airflow_P4ssw0rd.

### [AIRFLOW] ###
# Se define las variables de entorno para Airflow 
ENV AIRFLOW_HOME=/opt/airflow 
ENV PYTHON_VERSION=3.11
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Se establece el directorio de trabajo
WORKDIR /home/proyecto_matias_alvarez

# Se copian los archivos del proyecto
COPY ./data /home/proyecto_matias_alvarez/data
COPY ./data_services/airflow/capa_transformacion_python/src /home/proyecto_matias_alvarez/src
COPY ./data_services/airflow/capa_transformacion_python/tests /home/proyecto_matias_alvarez/tests
COPY ./data_services/airflow/scripts/ /home/proyecto_matias_alvarez/scripts

# Se da permisos de ejecución a todos los script bash
RUN chmod +x /home/proyecto_matias_alvarez/scripts/*

# Se copia el archivo requirements.txt al contenedor
COPY ./data_services/airflow/files/requirements.txt /home/proyecto_matias_alvarez

# Se instala las dependencias de Python desde requirements.txt 
RUN pip install --no-cache-dir -r /home/proyecto_matias_alvarez/requirements.txt

# Se instala las dependencias de sistema necesarias para Airflow
RUN apt-get update && apt-get upgrade -y && apt-get install -y\
     procps \
     nano \ 
     python3-dev \
     default-libmysqlclient-dev \
     gcc \
     build-essential \
     pkg-config 

# Se instala el conector de MySQL
RUN pip install --no-cache-dir mysqlclient==2.2.7

# Se instala Apache Airflow y sus dependencias
RUN pip install --no-cache-dir apache-airflow==2.10.3

# Se crean las carpetas necesarias para Airflow
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins

# Se instala el Provider de MSSQL
RUN pip install --no-cache-dir apache-airflow-providers-microsoft-mssql[common.sql]

# Se expone el puerto para el servidor web de Airflow
EXPOSE 8080

CMD ["/home/proyecto_matias_alvarez/scripts/iniciar_airflow.sh"]
