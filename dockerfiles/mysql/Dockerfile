FROM mysql:8.4.3

# Se carga configuracion para MySQL
COPY ./data_services/mysql/conf/my.cnf /etc/my.cnf

# Se carga los correspondientes scripts
COPY ./data_services/mysql/scripts/ /home/scripts/

# Se carga el Script para crear la base de datos de metadatos de airflow y su usuario
COPY ./data_services/mysql/scripts/sql/metadata_airflow.sql /docker-entrypoint-initdb.d/

# Se da permisos de ejecución a los script de bash
RUN chmod +x /home/scripts/bash/*

