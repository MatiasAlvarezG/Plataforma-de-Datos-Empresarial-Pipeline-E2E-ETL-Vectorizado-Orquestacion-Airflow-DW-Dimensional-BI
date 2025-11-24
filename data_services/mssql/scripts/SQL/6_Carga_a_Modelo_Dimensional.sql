use proyecto_matias_alvarez;
----------------------------------------------------------
-- DIM Fecha
----------------------------------------------------------
WITH todas_las_fechas AS (
	SELECT DISTINCT 
			IdFecha, 
			Fecha, 
			Fecha_Dia AS Dia, 
			Fecha_Mes AS Mes, 
			Fecha_Anio AS Anio,
			Nombre_Dia, 
			Numero_Dia_Semana, 
			Nombre_Mes, 
			Semana_Anio, 
			Fecha_Periodo AS Periodo, 
			Trimestre, 
			Feriado 
	FROM tmp_gasto
	UNION ALL
		SELECT DISTINCT 
			IdFecha, 
			Fecha, 
			Fecha_Dia AS Dia, 
			Fecha_Mes AS Mes, 
			Fecha_Anio AS Anio,
			Nombre_Dia, 
			Numero_Dia_Semana, 
			Nombre_Mes, 
			Semana_Anio, 
			Fecha_Periodo AS Periodo, 
			Trimestre, 
			Feriado 
		FROM tmp_compra
	UNION ALL
		SELECT DISTINCT 
			IdFecha_Venta AS IdFecha,
			Fecha_Venta AS Fecha,
			Fecha_Dia_Venta AS Dia, 
			Fecha_Mes_Venta AS Mes, 
			Fecha_Anio_Venta AS Venta, 
			Nombre_Dia_Venta AS Nombre_Dia, 
			Numero_Dia_Semana_Venta AS Numero_Dia_Semana, 
			Nombre_Mes_Venta AS Nombre_Mes, 
			Semana_Anio_Venta AS Num_Semana_Anio, 
			Fecha_Periodo_Venta AS Periodo, 
			Trimestre_Venta AS Trimestre, 
			Feriado_Venta AS Feriado
		FROM tmp_venta
)
INSERT INTO Dim_Fecha(
    IdFecha,
    Fecha,
    Dia,
    Mes,
    Anio,
    Nombre_Dia,
    Numero_Dia_Semana,
    Nombre_Mes,
    Num_Semana_Anio,
    Periodo,
    Trimestre,
    Feriado
) 
SELECT DISTINCT 
	*
FROM todas_las_fechas 
WHERE IdFecha NOT IN (SELECT IdFecha FROM Dim_Fecha);

-- Se actualiza la Fecha conocida propuesta como "Desconocida" para que todas sus partes 
UPDATE Dim_Fecha 
SET 
	Nombre_Dia = 'Desconocido',
	Nombre_Mes = 'Desconocido',
	Numero_Dia_Semana = -1,
	Num_Semana_Anio = -1,
	Trimestre = -1,
	Periodo = -1,
	Feriado = 0 
WHERE IdFecha = 99991231;

----------------------------------------------------------
-- DIM Producto
----------------------------------------------------------
INSERT INTO dim_producto(
	IdProducto,
	IdTipoProducto,
	Producto, 
	TipoProducto,
	Marca,
	Modelo,
	Componente,
	Precio,
	Producto_Descartado,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	CAST(IdProducto_Correcto AS INT) AS IdProducto,
	IdTipoProducto,
	Producto, 
	TipoProducto,
	Marca,
	Modelo,
	Componente,
	Precio,
	Producto_Descartado,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM tmp_producto
WHERE IdProducto = IdProducto_Correcto;

/*
-- Ejemplo prueba SDC tipo 2 
-- Observamos que pasa cuando modificamos el producto IdProducto = 42737 
INSERT INTO dim_producto(
	IdProducto,
	IdTipoProducto,
	Producto, 
	TipoProducto,
	Marca,
	Modelo,
	Componente,
	Precio,
	Producto_Descartado,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	42737 AS IdProducto,
	7,
	'Epson Copyfax 20001', 
	'Impresión',
        'Desconocido',
        'Ejemplo',
        'Ejemplo',
	2047,
	0,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual;
*/

-- select * from dim_producto WHERE IdProducto = 42737;
----------------------------------------------------------
-- DIM Proveedor
----------------------------------------------------------

-- SELECT * FROM dim_proveedor;

INSERT INTO dim_proveedor(
	IdProveedor,
	Sociedad_Completa,
	Nombre,
	Tipo_Sociedad,
	Direccion,
	Ciudad,
	Provincia,
	Pais,
        Departamento,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	CAST(IdProveedor_Correcto AS INT) AS IdCliente,
	Sociedad_Completa,
	Nombre,
	Tipo_Sociedad,
	Direccion,
	Ciudad,
	Provincia,
	Pais,
        Partido,	
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM tmp_proveedor
WHERE IdProveedor = IdProveedor_Correcto;

/* TEST SDC 
INSERT INTO dim_proveedor(
	IdProveedor,
	Sociedad_Completa,
	Nombre,
	Tipo_Sociedad,
	Direccion,
	Ciudad,
	Provincia,
	Pais,
        Departamento,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	IdProveedor,
	Sociedad_Completa,
	'Matias alvarez DE',
	Tipo_Sociedad,
	Direccion,
	Ciudad,
	Provincia,
	Pais,
        Departamento,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM Dim_Proveedor 
WHERE IdProveedor = 1 AND Version_Actual = 1;

SELECT * FROM dim_proveedor WHERE IdProveedor = 1;
*/

----------------------------------------------------------
-- DIM Sucursal
----------------------------------------------------------
INSERT INTO dim_sucursal(
	IdSucursal,
	Sucursal,
	Numero_Sucursal,
	Sucursal_completa,
	Direccion,
	Localidad,
	Provincia,
	Latitud,
	Longitud,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	CAST(IdSucursal_Correcto AS INT) AS IdSucursal,
	Sucursal,
	Numero_Sucursal,
	Sucursal_completa,
	Direccion,
	Localidad,
	Provincia,
	Latitud,
	Longitud,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM tmp_sucursal
WHERE IdSucursal = IdSucursal_Correcto;

/*TEST SDC Tipo 2
INSERT INTO dim_sucursal(
	IdSucursal,
	Sucursal,
	Numero_Sucursal,
	Sucursal_completa,
	Direccion,
	Localidad,
	Provincia,
	Latitud,
	Longitud,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	IdSucursal,
	Sucursal,
	Numero_Sucursal,
	Sucursal_completa,
	'Matias Alvarez Data Engineer 47',
	Localidad,
	Provincia,
	Latitud,
	Longitud,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM Dim_Sucursal
WHERE IdSucursal = 1 AND Version_Actual = 1;

SELECT * FROM dim_sucursal WHERE IdSucursal = 1;
*/
----------------------------------------------------------
-- DIM Cliente
----------------------------------------------------------
INSERT INTO dim_cliente(
	IdCliente,
	Nombre_y_Apellido,
	Nombre_Completo,
	Apellido_Completo,
	Nombre_1,
	Nombre_2,
	Nombre_3,
	Apellido_1,
	Apellido_2,
	Apellido_3,
	Edad,
	Grupo_Etario,
	Mayor_Edad,
	Domicilio,
	Provincia,
	Localidad,
	Latitud,
	Longitud,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	CAST(IdCliente_Correcto AS INT) AS IdCliente,
	Nombre_y_Apellido,
	Nombre_Completo,
	Apellido_Completo,
	Nombre_1,
	Nombre_2,
	Nombre_3,
	Apellido_1,
	Apellido_2,
	Apellido_3,
	Edad,
	Grupo_Etario,
	Mayor_Edad,
	Domicilio,
	Provincia,
	Localidad,
	Latitud,
	Longitud,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM tmp_cliente
WHERE IdCliente = IdCliente_Correcto;

/* TEST SCD TIPO 2
INSERT INTO dim_cliente(
	IdCliente,
	Nombre_y_Apellido,
	Nombre_Completo,
	Apellido_Completo,
	Nombre_1,
	Nombre_2,
	Nombre_3,
	Apellido_1,
	Apellido_2,
	Apellido_3,
	Edad,
	Grupo_Etario,
	Mayor_Edad,
	Domicilio,
	Provincia,
	Localidad,
	Latitud,
	Longitud,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	IdCliente,
	Nombre_y_Apellido,
	Nombre_Completo,
	Apellido_Completo,
	Nombre_1,
	Nombre_2,
	Nombre_3,
	Apellido_1,
	Apellido_2,
	Apellido_3,
	CASE 
		WHEN Edad=27 THEN 47
		ELSE 27 END AS Edad,
	Grupo_Etario,
	Mayor_Edad,
	Domicilio,
	Provincia,
	Localidad,
	Latitud,
	Longitud,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM Dim_Cliente
WHERE IdCliente = 1 AND Version_Actual = 1;
SELECT * FROM dim_cliente WHERE IdCliente = 1;

*/

----------------------------------------------------------
-- DIM Empleado
----------------------------------------------------------
INSERT INTO dim_empleado(
	IdEmpleado,
	Nombre_y_Apellido,
	Apellido,
	Nombre,
	Sucursal_Completa,
	Sucursal,
	Numero_Sucursal,
	Sector,
	Cargo,
	Salario,
	Sigue_Trabajando,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	IdEmpleado_Correcto,
	CONCAT(Nombre,' ',Apellido),
	Apellido,
	Nombre,
	Sucursal_Completa,
	Sucursal,
	Numero_Sucursal,
	Sector,
	Cargo,
	Salario,
	1 AS Sigue_Trabajando,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual
FROM tmp_empleado
WHERE IdEmpleado = IdEmpleado_Correcto;

/*TEST SCD
INSERT INTO dim_empleado(
	IdEmpleado,
	Nombre_y_Apellido,
	Apellido,
	Nombre,
	Sucursal_Completa,
	Sucursal,
	Numero_Sucursal,
	Sector,
	Cargo,
	Salario,
	Sigue_Trabajando,
	Alta,
	Baja,
	Version_Actual
)
SELECT 
	1011,
	'Luisa Sierra',
	'Sierra',
	'Luisa',
	'Palermo 2',
	'Palermo', 
	2,
	'Administración',
	'Vendedor',
	47000,
	1 AS Sigue_Trabajando,
	GETDATE() AS Alta,
	CAST('9999-12-31 23:59:59' AS DATETIME) AS Baja,
	1 AS Version_Actual;

select * from dim_empleado where IdEmpleado = 1011;
*/

----------------------------------------------------------
-- FACT Gastos
----------------------------------------------------------
INSERT INTO Fact_Gastos(
	IdGasto,
	SucursalSK,
	IdFecha,
	IdTipoGasto,
	TipoGasto,
	Monto, --Precio
	Outlier
)
SELECT
	g.IdGasto,
	s.SucursalSK,
	g.IdFecha,
	g.IdTipoGasto,
	g.Descripcion,
	g.Monto,
	g.Outlier
FROM 
	tmp_gasto g JOIN Dim_Sucursal s 
		ON g.IdSucursal = s.IdSucursal
		JOIN
	Dim_Fecha f 
		ON g.IdFecha = f.IdFecha
WHERE s.Version_Actual = 1;


----------------------------------------------------------
-- FACT Compras
----------------------------------------------------------

INSERT INTO Fact_Compras(
	IdCompra,
	ProductoSK,
	ProveedorSK,
	IdFecha,
	Cantidad,
	Precio,
	Compra_Total,
	Outlier
)
SELECT 
	c.IdCompra,
	prod.ProductoSK,
	prov.ProveedorSK,
	c.IdFecha,
	c.Cantidad,
	c.Precio,
	c.Compra_Total,
	c.Outlier
FROM tmp_compra c
	JOIN Dim_Producto prod
		ON c.IdProducto = prod.IdProducto
	JOIN Dim_Proveedor prov
		ON c.IdProveedor = prov.IdProveedor
	JOIN Dim_Fecha f
		ON c.IdFecha = f.IdFecha
WHERE 
	prod.Version_Actual = 1 AND
	prov.Version_Actual = 1;

----------------------------------------------------------
-- FACT Ventas
----------------------------------------------------------

INSERT INTO fact_ventas(
    IdVenta,
    ClienteSK,
    SucursalSK ,
    EmpleadoSK ,
    ProductoSK ,
    IdFecha_Venta ,
    IdFecha_Entrega ,
    Dias_Entregado,
    Precio,
    Cantidad,
    Compra_Total,
    Outlier,
	IdCanal,
	Tipo_Canal
)
SELECT
    v.IdVenta,
    cl.ClienteSK,
    suc.SucursalSK ,
    emp.EmpleadoSK ,
    prod.ProductoSK ,
    v.IdFecha_Venta ,
    v.IdFecha_Entrega ,
    v.Dias_Entregado,
    v.Precio,
    v.Cantidad,
    v.Compra_Total,
    v.Outlier,
	v.IdCanal,
	v.Tipo_Canal_Venta
FROM tmp_venta v
	JOIN Dim_Producto prod
		ON v.IdProducto = prod.IdProducto
	JOIN Dim_Cliente cl
		ON v.IdCliente = cl.IdCliente
	JOIN Dim_Sucursal suc
		ON v.IdSucursal = suc.IdSucursal
	JOIN Dim_Empleado emp
		ON v.IdEmpleado = emp.IdEmpleado
	JOIN Dim_Fecha f_venta
		ON v.IdFecha_Venta = f_venta.IdFecha
	JOIN Dim_Fecha f_entrega
		ON v.IdFecha_Entrega = f_entrega.IdFecha
WHERE 
	prod.Version_Actual = 1 AND
	cl.Version_Actual = 1 AND
	suc.Version_Actual = 1 AND
	emp.Version_Actual = 1;
