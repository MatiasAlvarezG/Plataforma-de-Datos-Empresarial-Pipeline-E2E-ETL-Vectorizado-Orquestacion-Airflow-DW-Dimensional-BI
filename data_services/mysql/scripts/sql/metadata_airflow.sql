-- Se crea la DB de metadatos para Airflow. 
CREATE DATABASE IF NOT EXISTS airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE airflow_db;
 
-- Se crea el usuario para airflow
CREATE USER IF NOT EXISTS 'airflow_user'@'%' IDENTIFIED WITH mysql_native_password BY 'pass_word_airflow_db';

-- Se otorgan permisos completos
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user'@'%';

FLUSH PRIVILEGES;
