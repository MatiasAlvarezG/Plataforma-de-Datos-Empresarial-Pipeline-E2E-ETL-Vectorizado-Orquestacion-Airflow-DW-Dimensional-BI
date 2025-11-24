USE master;
GO
-- Se crea un usuario para verificar las bases de datos creadas. 
-- El mismo será de utilidad en el contenedor de airflow para verificar si existe la base de datos del proyecto.
CREATE LOGIN user_verificacion_db WITH PASSWORD = 'User_V3r1ficaci0n.';

CREATE USER user_verificacion_db FOR LOGIN user_verificacion_db;

-- Privilegios solo de vista 
GRANT VIEW ANY DATABASE TO user_verificacion_db;

GO

