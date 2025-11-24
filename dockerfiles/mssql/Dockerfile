FROM mcr.microsoft.com/mssql/server:2022-latest

WORKDIR /home

# Se copian los scripts SQL desde el host al contenedor en la ruta /home/data/sql/scripts
COPY ./data_services/mssql/scripts/SQL /home/scripts

# Se copian el archivo de configuración y otros datos que se necesiten
COPY ./data_services/mssql/dataset /home/data

ENV DATABASE_SQL_COM='comunication_db'
ENV DATABASE_SQL_DIMENSIONAL='proyecto_matias_alvarez'
ENV DB_USER_ADM='sa'
ENV DB_PWD_ADM='Sqlserver2024)'

USER root

RUN apt-get update && apt-get install -y python3-pip

# Se copian los scripts para la creación de la DB, tablas, procedimientos, triggers y usuarios
COPY ./data_services/mssql/scripts/bash/ /home/bash_script/

# Se copia el script Python que escucha las peticiones del contendor Airflow
COPY ./data_services/mssql/scripts/python/ /home/python/

# Se da privilegios de ejecución a los scripts
RUN chmod +x /home/bash_script/*

RUN chown -R mssql:mssql /home/bash_script
RUN chown -R mssql:mssql /home/python

# Se establecen variables de entornos necesarias
ENV ACCEPT_EULA=Y
ENV MSSQL_PID=Express
ENV MSSQL_SA_PASSWORD=Sqlserver2024)

# Se instala las herramientas de mssql-tools para poder ejecutar sqlcmd
RUN ACCEPT_EULA=Y && \
    apt-get update && apt-get install -y \
    mssql-tools18 \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pymssql==2.3.4

USER mssql

# Se crean todas las bases de datos necesarias junto con todas las necesidades de la misma y por último la base de datos para la comunicación entre servicios.
RUN /home/bash_script/crear_usuario_verificacion.sh sleep 10 && /home/bash_script/cargar_script.sh && sleep 1 && /home/bash_script/crear_db_para_eventos.sh




