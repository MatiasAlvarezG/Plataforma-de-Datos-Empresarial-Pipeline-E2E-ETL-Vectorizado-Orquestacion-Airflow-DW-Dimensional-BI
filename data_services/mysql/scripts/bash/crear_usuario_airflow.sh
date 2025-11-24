#!/bin/bash
# crear_usuario_airflow.sh

MSG="con éxito el usuario para la db de metadatos de Apache Airflow!"

# Si se utiliza el modo  mysql_native_password debe ingresarse un usuario y contraseña, caso contrario es solo: mysql < "RUTA_SCRIPT.sql"
/usr/bin/mysql -u root -p$MYSQL_ROOT_PASSWROD < "/home/scripts/sql/metadata_airflow.sql"

# Verificar si el script se ejecutó correctamente
if [ $? -eq 0 ]; then
   echo "INFO ::: Se creó $MSG"
   exit 0
else
   echo "ERROR ::: No se pudo ejecutar $MSG"
   exit 1
fi