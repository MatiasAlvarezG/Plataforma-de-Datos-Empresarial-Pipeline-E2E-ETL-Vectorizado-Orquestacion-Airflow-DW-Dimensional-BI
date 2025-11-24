use proyecto_matias_alvarez;

-------------------------------------------------------------------------------------------------------------------------
-- Usuario para Apache Airflow
-------------------------------------------------------------------------------------------------------------------------
-- Se crea inicio de sesión para la instancia de SQL Server
CREATE LOGIN python WITH PASSWORD = 'Pyth0N_p4ssw0rd.';

-- Se crea el usuario python
CREATE USER python FOR LOGIN python;
GO

EXEC sp_addrolemember 'db_datawriter', 'python';
GO

-------------------------------------------------------------------------------------------------------------------------
-- Usuario para servicio de análisis de datos y dashboard
-------------------------------------------------------------------------------------------------------------------------

-- Se crea inicio de sesión para la instancia de SQL Server
CREATE LOGIN dashboard WITH PASSWORD = 'D4shBoard)2024'; 

-- Se crea el usuario dashboard
CREATE USER dashboard FOR LOGIN dashboard;

-- Se agrega los privilegios SELECT y VIEW DEFINITION para el usuario
GRANT SELECT, VIEW DEFINITION ON dbo.Dim_Producto TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Dim_Cliente TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Dim_Empleado TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Dim_Fecha TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Dim_Proveedor TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Dim_Sucursal TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Fact_Compras TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Fact_Gastos TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.Fact_Ventas TO dashboard;
GRANT SELECT, VIEW DEFINITION ON dbo.tmp_venta TO dashboard;