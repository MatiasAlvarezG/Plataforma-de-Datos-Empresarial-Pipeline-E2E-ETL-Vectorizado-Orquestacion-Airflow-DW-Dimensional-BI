FROM apache/superset:3.1.3

WORKDIR /app

# Se copia cualquier archivo adicional
COPY ./data_services/superset /home/data 

# Se establecen las variables de entornos necesarias
ENV SUPERSET_SECRET_KEY=apache_super_set_proyecto_matias_alvarez
ENV SQLALCHEMY_DATABASE_URI=sqlite:////var/lib/superset/superset.db  
ENV SUPERSET_LOAD_EXAMPLES=False
ENV SUPPORTED_DATABASES=mssql  
ENV DB_NAME=proyecto_matias_alvarez
ENV DB_USER_MOD_DIM=dashboard
ENV DB_PWD_MOD_DIM=D4shBoard)2024

USER root

# Se instala pymssql para la conexión a SQL Server
RUN pip install pymssql

# Se da privilegio de ejecución al script que crea e inicializa la db de metadatos y crea el usuario:superset con pw:superset
# Y se inicializa el script
RUN chmod +x /home/data/crear_db.sh && /home/data/crear_db.sh && chown superset:superset /app/superset_home/superset.db

USER superset






