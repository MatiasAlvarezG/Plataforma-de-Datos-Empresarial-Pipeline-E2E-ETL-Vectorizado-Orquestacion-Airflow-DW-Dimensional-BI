#!/bin/bash
# crear_db.sh

# Se crea el usuario admin "superset" con contraseña "superset"
/usr/local/bin/superset fab create-admin \
	              --username superset \
        	      --firstname Matias \
	              --lastname Alvarez \
        	      --email admin@matias_alvarez_data_engineer.com \
	              --password superset

# Se actualiza la base de datos de metadatos 
/usr/local/bin/superset db upgrade

# Se inicia
/usr/local/bin/superset init

sleep 20
# Se crea la conexión para la base de datos dimensional del proyecto
/usr/local/bin/superset set-database-uri --database-name "MSSQL_DIMENSIONAL" --uri "mssql+pymssql://$DB_USER_MOD_DIM:$DB_PWD_MOD_DIM@sql_server:1433/$DB_NAME"
