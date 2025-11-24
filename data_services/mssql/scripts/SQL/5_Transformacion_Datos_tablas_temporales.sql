USE proyecto_matias_alvarez;

/*########################################################################
 ######################### ÁREA DE PREPARACIÓN ###########################
 ########################################################################*/

----------------------------------------------------------
-- Tipo Producto
----------------------------------------------------------

-- select * from tmp_tipo_producto;

/* Se observan cantidad de errores */

/* 
SELECT 
    SUM(CASE WHEN Error_IdTipoProducto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdTipoProducto,
    SUM(CASE WHEN Error_TipoProducto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_TipoProducto
FROM 
    tmp_tipo_producto;
*/

-- Se procesan los posibles duplicado no directos
EXEC procesar_dup_nd_tipo_producto;

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_tipo_producto', 'IdTipoProducto'

-- Se almacena los errores

-- (1) IdTipoProducto 
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_producto' AS Tabla,
	'Error_IdTipoProducto' AS Columna,
	IdTipoProducto AS IdPrincipal,
	IdTipoProducto AS Valor_Almacenado,
	Error_IdTipoProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_producto
WHERE Error_IdTipoProducto != 'Desconocido';

-- (2) Error_TipoProducto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_producto' AS Tabla,
	'Error_TipoProducto' AS Columna,
	IdTipoProducto AS IdPrincipal,
	TipoProducto AS Valor_Almacenado,
	Error_TipoProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_producto
WHERE Error_TipoProducto != 'Desconocido';

-- (3) IdTipoProducto dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_producto' AS Tabla,
	'DUP_ND' AS Columna,
	IdTipoProducto AS IdPrincipal,
	IdTipoProducto_Correcto AS Valor_Almacenado,
	IdTipoProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_producto
WHERE IdTipoProducto_Descartado = 1;

-- (4) IdTipoProducto duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_producto' AS Tabla,
	'DUP_D' AS Columna,
	IdTipoProducto AS IdPrincipal,
	IdTipoProducto_Correcto AS Valor_Almacenado,
	Error_IdTipoProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_producto
WHERE IdTipoProducto_Actualizado = 1;
----------------------------------------------------------
-- Producto
----------------------------------------------------------

-- SELECT * FROM tmp_tipo_producto;
-- SELECT * FROM tmp_producto;

/* Se observan cantidad de errores */
/*
SELECT 
    SUM(CASE WHEN Error_IdProducto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdProducto,
    SUM(CASE WHEN Error_IdTipoProducto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdTipoProducto,
    SUM(CASE WHEN Error_Precio != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Precio
FROM 
    tmp_producto;
*/

/* Se agrega Tipo Producto y si el Producto esta descartado y tambien su tipo producto*/
UPDATE p
SET 
	p.TipoProducto = tp.TipoProducto,
	p.TipoProducto_Descartado = tp.TipoProducto_Descartado,
	p.Producto_Descartado = CASE 
		WHEN p.IdProducto IN (42802, 42803,42804, 42805, 42806, 42807,42808, 42809) THEN 1 
		ELSE 0 END
FROM tmp_producto p JOIN tmp_tipo_producto tp 
	ON p.IdTipoProducto = tp.IdTipoProducto;


/* Outlier */
-- Ya procesado en python 


-- Procesar No Duplicados
EXEC procesar_dup_nd_producto;  --procesar_dup_nd_producto

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_producto', 'IdProducto'

-- Se almacena los errores

-- (1) IdProducto 
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_producto' AS Tabla,
	'Error_IdProducto' AS Columna,
	IdProducto AS IdPrincipal,
	IdProducto AS Valor_Almacenado,
	Error_IdProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_producto
WHERE Error_IdProducto != 'Desconocido';

-- (2) Precio
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_producto' AS Tabla,
	'Error_Precio' AS Columna,
	IdProducto AS IdPrincipal,
	Precio AS Valor_Almacenado,
	Error_Precio AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_producto
WHERE Error_Precio != 'Desconocido';

-- (3) IdTipoProducto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_producto' AS Tabla,
	'Error_IdTipoProducto' AS Columna,
	IdProducto AS IdPrincipal,
	IdTipoProducto AS Valor_Almacenado,
	Error_IdTipoProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_producto
WHERE Error_IdTipoProducto != 'Desconocido';

-- (4) Tipo Producto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_producto' AS Tabla,
	'Error_TipoProducto' AS Columna,
	IdProducto AS IdPrincipal,
	TipoProducto AS Valor_Almacenado,
	Error_TipoProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_producto
WHERE Error_TipoProducto != 'Desconocido';

-- (5) IdProducto dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_producto' AS Tabla,
	'DUP_ND' AS Columna,
	IdProducto AS IdPrincipal,
	IdProducto_Correcto AS Valor_Almacenado,
	IdProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_producto
WHERE IdProducto_Descartado = 1;

-- (6) IdProducto duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_producto' AS Tabla,
	'DUP_D' AS Columna,
	IdProducto AS IdPrincipal,
	IdProducto_Correcto AS Valor_Almacenado,
	Error_IdProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_producto
WHERE IdProducto_Actualizado = 1;
----------------------------------------------------------
-- Canal Venta
----------------------------------------------------------

-- SELECT * FROM tmp_canal_venta;

-- Se observa errores de la tabla
/*
SELECT 
    SUM(CASE WHEN Error_IdCanal != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdCanal
FROM 
    tmp_canal_venta;
*/

-- Procesar No Duplicados
EXEC procesar_dup_nd_canal_venta; 


-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_canal_venta', 'IdCanal'

-- Se almacena los errores

-- (1) IdCanal 
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_canal_venta' AS Tabla,
	'Error_IdCanal' AS Columna,
	IdCanal AS IdPrincipal,
	IdCanal AS Valor_Almacenado,
	Error_IdCanal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_canal_venta
WHERE Error_IdCanal != 'Desconocido' AND IdCanal_Actualizado = 0;

-- (2) IdCanal dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_canal_venta' AS Tabla,
	'DUP_ND' AS Columna,
	IdCanal AS IdPrincipal,
	IdCanal_Correcto AS Valor_Almacenado,
	IdCanal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_canal_venta
WHERE IdCanal_Descartado = 1;

-- (3) IdCanal duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_canal_venta' AS Tabla,
	'DUP_D' AS Columna,
	IdCanal AS IdPrincipal,
	IdCanal_Correcto AS Valor_Almacenado,
	Error_IdCanal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_canal_venta
WHERE IdCanal_Actualizado = 1;
----------------------------------------------------------
-- Tipo Gasto
----------------------------------------------------------

-- SELECT * FROM tmp_tipo_gasto;

-- Se observa errores de la tabla
/*
SELECT 
    SUM(CASE WHEN Error_IdTipoGasto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdTipoGasto,
    SUM(CASE WHEN Error_Descripcion != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Descripcion,
    SUM(CASE WHEN Error_Monto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Monto
FROM 
    tmp_tipo_gasto;
*/

-- Procesar No Duplicados
EXEC procesar_dup_nd_tipo_gasto; 

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_tipo_gasto', 'IdTipoGasto'

-- Se almacena los errores
-- (1) IdTipoGasto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_gasto' AS Tabla,
	'Error_IdTipoGasto' AS Columna,
	IdTipoGasto AS IdPrincipal,
	IdTipoGasto AS Valor_Almacenado,
	Error_IdTipoGasto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_gasto
WHERE Error_IdTipoGasto != 'Desconocido'  AND IdTipoGasto_Actualizado = 0;

-- (2) Error_Descripcion
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_gasto' AS Tabla,
	'Error_Descripcion' AS Columna,
	IdTipoGasto AS IdPrincipal,
	Descripcion AS Valor_Almacenado,
	Error_Descripcion AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_gasto
WHERE Error_Descripcion != 'Desconocido';

-- (3) Error_Monto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_gasto' AS Tabla,
	'Error_Monto' AS Columna,
	IdTipoGasto AS IdPrincipal,
	Monto_Aproximado AS Valor_Almacenado,
	Error_Monto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_gasto
WHERE Error_Monto != 'Desconocido';

-- (4) TipoGasto dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_gasto' AS Tabla,
	'DUP_ND' AS Columna,
	IdTipoGasto AS IdPrincipal,
	IdTipoGasto_Correcto AS Valor_Almacenado,
	IdTipoGasto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_gasto
WHERE IdTipoGasto_Descartado = 1;

-- (5) IdTipoGasto duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_tipo_gasto' AS Tabla,
	'DUP_D' AS Columna,
	IdTipoGasto AS IdPrincipal,
	IdTipoGasto_Correcto AS Valor_Almacenado,
	Error_IdTipoGasto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_tipo_gasto
WHERE IdTipoGasto_Actualizado = 1;
----------------------------------------------------------
-- Proveedor
----------------------------------------------------------

-- SELECT * FROM tmp_proveedor;

-- Se observa errores de la tabla
/*
SELECT 
    SUM(CASE WHEN Error_IdProveedor != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdProveedor
FROM 
    tmp_proveedor;
*/

-- Duplicados no directos
EXEC procesar_dup_nd_proveedor;

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_proveedor', 'IdProveedor'

-- Se almacena los errores
-- (1) IdProveedor
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_proveedor' AS Tabla,
	'Error_IdProveedor' AS Columna,
	IdProveedor AS IdPrincipal,
	IdProveedor AS Valor_Almacenado,
	Error_IdProveedor AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_proveedor
WHERE Error_IdProveedor != 'Desconocido' AND IdProveedor_Actualizado = 0;

-- (2) Proveedor dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_proveedor' AS Tabla,
	'DUP_ND' AS Columna,
	IdProveedor AS IdPrincipal,
	IdProveedor_Correcto AS Valor_Almacenado,
	IdProveedor AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_proveedor
WHERE IdProveedor_Descartado = 1;

-- (3) IdProveedor duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_proveedor' AS Tabla,
	'DUP_D' AS Columna,
	IdProveedor AS IdPrincipal,
	IdProveedor_Correcto AS Valor_Almacenado,
	Error_IdProveedor AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_proveedor
WHERE IdProveedor_Actualizado = 1;
----------------------------------------------------------
-- Sucursal
----------------------------------------------------------

-- select * from tmp_sucursal;

-- Se observa errores de la tabla
/*
SELECT 
	SUM(CASE WHEN Error_IdSucursal != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdSucursal,
    SUM(CASE WHEN Error_Latitud != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Latitud,
    SUM(CASE WHEN Error_Longitud != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Longitud
FROM 
    tmp_sucursal;
*/

-- Procesar No Duplicados
EXEC procesar_dup_nd_sucursal; 

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_sucursal', 'IdSucursal'

-- Se almacena los errores

-- (1) IdSucursal
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_sucursal' AS Tabla,
	'Error_IdSucursal' AS Columna,
	IdSucursal AS IdPrincipal,
	IdSucursal AS Valor_Almacenado,
	Error_IdSucursal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_sucursal
WHERE Error_IdSucursal != 'Desconocido';

-- (2) Latitud
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_sucursal' AS Tabla,
	'Error_Latitud' AS Columna,
	IdSucursal AS IdPrincipal,
	Latitud AS Valor_Almacenado,
	Error_Latitud AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_sucursal
WHERE Error_Latitud != 'Desconocido';

-- (3) Longitud
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_sucursal' AS Tabla,
	'Error_Longitud' AS Columna,
	IdSucursal AS IdPrincipal,
	Longitud AS Valor_Almacenado,
	Error_Longitud AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_sucursal
WHERE Error_Longitud != 'Desconocido';

-- (3) IdSucursal dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_sucursal' AS Tabla,
	'DUP_ND' AS Columna,
	IdSucursal AS IdPrincipal,
	IdSucursal_Correcto AS Valor_Almacenado,
	IdSucursal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_sucursal
WHERE IdSucursal_Descartado = 1;

-- (4) IdSucursal duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_sucursal' AS Tabla,
	'DUP_D' AS Columna,
	IdSucursal AS IdPrincipal,
	IdSucursal_Correcto AS Valor_Almacenado,
	Error_IdSucursal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_sucursal
WHERE IdSucursal_Actualizado = 1;
----------------------------------------------------------
-- Empleado
----------------------------------------------------------

-- SELECT * FROM tmp_empleado;

-- Se observa errores de la tabla
/*
SELECT 
    SUM(CASE WHEN Error_IdEmpleado != 'Desconocido' AND IdEmpleado_Actualizado = 0 THEN 1 ELSE 0 END) AS CANT_Error_IdEmpleado,
    SUM(CASE WHEN Error_Sucursal != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Sucursal,
    SUM(CASE WHEN Error_Salario != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Salario
FROM 
    tmp_empleado;
*/

-- Procesar No Duplicados
EXEC procesar_dup_nd_empleado;

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_empleado', 'IdEmpleado'

-- Se almacena los errores
-- (1) IdEmpleado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_empleado' AS Tabla,
	'Error_IdEmpleado' AS Columna,
	IdEmpleado AS IdPrincipal,
	IdEmpleado AS Valor_Almacenado,
	Error_IdEmpleado AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_empleado
WHERE Error_IdEmpleado != 'Desconocido' AND IdEmpleado_Actualizado = 0;

-- (2) Sucursal
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_empleado' AS Tabla,
	'Error_Sucursal' AS Columna,
	IdEmpleado AS IdPrincipal,
	Sucursal AS Valor_Almacenado,
	Error_Sucursal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_empleado
WHERE Error_Sucursal != 'Desconocido';

-- (3) Salario
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_empleado' AS Tabla,
	'Error_Salario' AS Columna,
	IdEmpleado AS IdPrincipal,
	Salario AS Valor_Almacenado,
	Error_Salario AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_empleado
WHERE Error_Salario != 'Desconocido';

-- (3) IdEmpleado dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_empleado' AS Tabla,
	'DUP_ND' AS Columna,
	IdEmpleado AS IdPrincipal,
	IdEmpleado_Correcto AS Valor_Almacenado,
	IdEmpleado AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_empleado
WHERE IdEmpleado_Descartado = 1;

-- (4) IdEmpleado duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_empleado' AS Tabla,
	'DUP_D' AS Columna,
	IdEmpleado AS IdPrincipal,
	IdEmpleado_Correcto AS Valor_Almacenado,
	Error_IdEmpleado AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_empleado
WHERE IdEmpleado_Actualizado = 1;

----------------------------------------------------------
-- Cliente
----------------------------------------------------------

-- SELECT * FROM tmp_cliente WHERE Error_Edad != 'Desconocido' ;

-- Se observa errores de la tabla
/*
SELECT 
	SUM(CASE WHEN Error_IdCliente != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdCliente,
    SUM(CASE WHEN Error_Edad != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Edad,
    SUM(CASE WHEN Error_Longitud != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Longitud,
    SUM(CASE WHEN Error_Latitud != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Latitud
FROM 
    tmp_cliente;
*/

-- Procesar No Duplicados
EXEC procesar_dup_nd_cliente;

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_cliente', 'IdCliente'

-- Se almacena los errores
-- (1) IdCliente
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_cliente' AS Tabla,
	'Error_IdCliente' AS Columna,
	IdCliente AS IdPrincipal,
	IdCliente AS Valor_Almacenado,
	Error_IdCliente AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_cliente
WHERE Error_IdCliente != 'Desconocido' AND IdCliente_Actualizado = 0;

-- (2) Edad
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_cliente' AS Tabla,
	'Error_Edad' AS Columna,
	IdCliente AS IdPrincipal,
	Edad AS Valor_Almacenado,
	Error_Edad AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_cliente
WHERE Error_Edad != 'Desconocido';

-- (3) Longitud
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_cliente' AS Tabla,
	'Error_Longitud' AS Columna,
	IdCliente AS IdPrincipal,
	Longitud AS Valor_Almacenado,
	Error_Longitud AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_cliente
WHERE Error_Longitud != 'Desconocido';

-- (4) Latitud
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_cliente' AS Tabla,
	'Error_Latitud' AS Columna,
	IdCliente AS IdPrincipal,
	Latitud AS Valor_Almacenado,
	Error_Latitud AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_cliente
WHERE Error_Latitud != 'Desconocido';

-- (5) IdCliente dup no directo
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_cliente' AS Tabla,
	'DUP_ND' AS Columna,
	IdCliente AS IdPrincipal,
	IdCliente_Correcto AS Valor_Almacenado,
	IdCliente AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_cliente
WHERE IdCliente_Descartado = 1;

-- (6) IdCliente duplicado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_cliente' AS Tabla,
	'DUP_D' AS Columna,
	IdCliente AS IdPrincipal,
	IdCliente_Correcto AS Valor_Almacenado,
	Error_IdCliente AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_cliente
WHERE IdCliente_Actualizado = 1;

----------------------------------------------------------
-- Gasto
----------------------------------------------------------

-- select * from tmp_gasto;

-- Se observa errores de la tabla
/*
SELECT 
	SUM(CASE WHEN Error_IdGasto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdGasto,
    SUM(CASE WHEN Error_IdSucursal != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdSucursal,
    SUM(CASE WHEN Error_IdTipoGasto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdTipoGasto,
	SUM(CASE WHEN Error_Monto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Monto,
	SUM(CASE WHEN Error_Fecha != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Fecha
FROM 
    tmp_gasto;
*/

-- Se agrega Descripción y Monto_Aproximado
UPDATE g
SET g.Descripcion = tp.Descripcion,
    g.Monto_Aproximado_Tipo = tp.Monto_Aproximado
FROM tmp_gasto g JOIN tmp_tipo_gasto tp ON g.IdTipoGasto = tp.IdTipoGasto;

-- Aplicar Feriado (realizado en python)
-- Outlier (realizado en python)

-- Actualizar IdTipoGasto erroneos (por duplicado no directo)
EXEC actualizar_id_de_dup_nd 'tmp_gasto', 'tmp_tipo_gasto', 'IdTipoGasto';

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_gasto', 'IdGasto'

-- Se almacenan los errores
-- (1) IdGasto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_gasto' AS Tabla,
	'Error_IdGasto' AS Columna,
	IdGasto AS IdPrincipal,
	IdGasto AS Valor_Almacenado,
	Error_IdGasto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_gasto
WHERE Error_IdGasto != 'Desconocido';

-- (2) IdSucursal
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_gasto' AS Tabla,
	'Error_IdSucursal' AS Columna,
	IdGasto AS IdPrincipal,
	IdSucursal AS Valor_Almacenado,
	Error_IdSucursal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_gasto
WHERE Error_IdSucursal != 'Desconocido';

-- (3) IdTipoGasto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_gasto' AS Tabla,
	'Error_IdTipoGasto' AS Columna,
	IdGasto AS IdPrincipal,
	IdTipoGasto AS Valor_Almacenado,
	Error_IdTipoGasto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_gasto
WHERE Error_IdTipoGasto != 'Desconocido';

-- (4) Monto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_gasto' AS Tabla,
	'Error_Monto' AS Columna,
	IdGasto AS IdPrincipal,
	Monto AS Valor_Almacenado,
	Error_Monto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_gasto
WHERE Error_Monto != 'Desconocido' AND Outlier=0;

-- (5) Fecha
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_gasto' AS Tabla,
	'Error_Fecha' AS Columna,
	IdGasto AS IdPrincipal,
	Fecha AS Valor_Almacenado,
	Error_Fecha AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_gasto
WHERE Error_Fecha != 'Desconocido';

-- (6) Outlier
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_gasto' AS Tabla,
	'OUTLIER' AS Columna,
	IdGasto AS IdPrincipal,
	Monto AS Valor_Almacenado,
	Error_Monto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_gasto
WHERE Error_Monto != 'Desconocido' AND Outlier=1;


----------------------------------------------------------
-- Compra
----------------------------------------------------------
-- select * from tmp_compra;

-- Se observa errores de la tabla
/*
SELECT 
	SUM(CASE WHEN Error_IdCompra != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdCompra,
    SUM(CASE WHEN Error_IdProducto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdProducto,
    SUM(CASE WHEN Error_Cantidad != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Cantidad,
	SUM(CASE WHEN Error_Precio != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Precio,
	SUM(CASE WHEN Error_Fecha != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Fecha,
	SUM(CASE WHEN Error_IdProveedor != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdProveedor
FROM 
    tmp_compra;
*/

-- Aplicar Feriado (realizado en python)
-- Outlier (realizado en python)

-- Actualizar IdProveedores erroneos (por duplicado no directo)
EXEC actualizar_id_de_dup_nd 'tmp_compra', 'tmp_proveedor', 'IdProveedor';

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_compra', 'IdCompra'

-- Se solucionan posibles casos resultantes donde el precio del producto es erroneo y no fue solucionado.
UPDATE c
SET c.Precio = p.Precio, c.Compra_Total = p.Precio * c.Cantidad
FROM 
    tmp_compra c 
INNER JOIN 
    tmp_producto p ON c.IdProducto = p.IdProducto
WHERE 
    c.Precio = -1 AND p.Producto_Descartado=0;


-- Se normaliza cualquier caso donde Compra_Total sea menor que -1
UPDATE tmp_venta
SET Compra_Total = 0 -- Anteriormente se utilizaba el valor -1 pero para evitar problemas ahora se utiliza 0
WHERE Compra_Total <= -1;


-- Se almacenan los errores
-- (1) IdCompra
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'Error_IdCompra' AS Columna,
	IdCompra AS IdPrincipal,
	IdCompra AS Valor_Almacenado,
	Error_IdCompra AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_IdCompra != 'Desconocido';

-- (2) IdProducto
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'Error_IdProducto' AS Columna,
	IdCompra AS IdPrincipal,
	IdProducto AS Valor_Almacenado,
	Error_IdProducto AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_IdProducto != 'Desconocido';

-- (3) Cantidad
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'Error_Cantidad' AS Columna,
	IdCompra AS IdPrincipal,
	Cantidad AS Valor_Almacenado,
	Error_Cantidad AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_Cantidad != 'Desconocido';

-- (4) Precio
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'Error_Precio' AS Columna,
	IdCompra AS IdPrincipal,
	Precio AS Valor_Almacenado,
	Error_Precio AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_Precio != 'Desconocido' AND Outlier=0;

-- (5) Fecha
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'Error_Fecha' AS Columna,
	IdCompra AS IdPrincipal,
	Fecha AS Valor_Almacenado,
	Error_Fecha AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_Fecha != 'Desconocido';

-- (6) IdProveedor
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'Error_IdProveedor' AS Columna,
	IdCompra AS IdPrincipal,
	IdProveedor AS Valor_Almacenado,
	Error_IdProveedor AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_IdProveedor != 'Desconocido';

-- (7) Outlier
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_compra' AS Tabla,
	'OUTLIER' AS Columna,
	IdCompra AS IdPrincipal,
	Precio AS Valor_Almacenado,
	Error_Precio AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_compra
WHERE Error_Precio != 'Desconocido' AND Outlier=1;

----------------------------------------------------------
-- Venta
----------------------------------------------------------
-- SELECT TOP 1 * FROM tmp_venta;
-- SELECT * FROM tmp_venta;

-- Se observa errores de la tabla
/*
SELECT 
	SUM(CASE WHEN Error_IdVenta != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdVenta,
    SUM(CASE WHEN Error_IdProducto != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdProducto,
    SUM(CASE WHEN Error_Cantidad != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Cantidad,
	SUM(CASE WHEN Error_IdCanal != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdCanal,
	SUM(CASE WHEN Error_IdCliente != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdCliente,
	SUM(CASE WHEN Error_IdSucursal != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdSucursal,
	SUM(CASE WHEN Error_Precio != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Precio,
	SUM(CASE WHEN Error_Fecha_Venta != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Fecha_Venta,
	SUM(CASE WHEN Error_Fecha_Entrega != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_Fecha_Entrega,
	SUM(CASE WHEN Error_IdEmpleado != 'Desconocido' THEN 1 ELSE 0 END) AS CANT_Error_IdEmpleado
FROM 
    tmp_venta;
*/

-- Feriados  (realizado en python)

/* Actualiza los IdEmpleados en registros con ID duplicados.
   Se identifican los duplicados basándose en el IdEmpleado duplicado y la IdSucursal de la venta.
   Entonces se buscan los IdEmpleados marcados como duplicados (Id_Actualizado = 1 en la tabla de empleados) que tienen un nuevo ID asignado.
   Si existe una venta con un IdEmpleado problemático y esta pertenece a la sucursal del nuevo ID asignado,
   se actualiza el IdEmpleado con el nuevo valor correspondiente. */ 

-- Total de IDs a Actualizar
-- SELECT COUNT(IdEmpleado_Actualizado) AS Total_Actualizar FROM tmp_empleado WHERE IdEmpleado_Actualizado = 1;

-- Parte 1
WITH empleado_a_actualizar AS (
    SELECT 
        e.IdEmpleado, 
        e.Sucursal_Completa, 
        s.IdSucursal, 
        e.Error_IdEmpleado 
    FROM 
        tmp_empleado e 
    JOIN tmp_sucursal s 
        ON e.Sucursal_Completa = s.Sucursal_Completa 
    WHERE 
        IdEmpleado_Actualizado = 1
)
-- Se almacena previamente el error
UPDATE v
SET 
    v.Error_IdEmpleado = v.IdEmpleado
FROM 
    tmp_venta v 
INNER JOIN 
    empleado_a_actualizar e ON v.IdEmpleado = e.Error_IdEmpleado
WHERE 
    v.IdSucursal = e.IdSucursal;

-- Ahora se actualiza IdEmpleado ya que UPDATE en multiple sentencias ejecuta todas a la vez haciendo que posiblemente almacene un valor erroneo.

-- Parte 2
WITH empleado_a_actualizar AS (
    SELECT 
        e.IdEmpleado, 
        e.Sucursal_Completa, 
        s.IdSucursal, 
        e.Error_IdEmpleado 
    FROM 
        tmp_empleado e 
    JOIN tmp_sucursal s 
        ON e.Sucursal_Completa = s.Sucursal_Completa 
    WHERE 
        IdEmpleado_Actualizado = 1
)
UPDATE v
SET 
    v.IdEmpleado = e.IdEmpleado
FROM 
    tmp_venta v 
INNER JOIN 
    empleado_a_actualizar e ON v.IdEmpleado = e.Error_IdEmpleado
WHERE 
    v.IdSucursal = e.IdSucursal;

-- Se observa si hay que actualizar Proveedores
-- SELECT COUNT(*) AS Total_Actualizar FROM tmp_proveedor WHERE Error_IdProveedor != 'Desconocido' ;

-- Actualizar IdTipoGasto erroneos (por duplicado no directo)
EXEC actualizar_id_de_dup_nd 'tmp_venta', 'tmp_canal_venta', 'IdCanal';
EXEC actualizar_id_de_dup_nd 'tmp_venta', 'tmp_cliente', 'IdCliente';
EXEC actualizar_id_de_dup_nd 'tmp_venta', 'tmp_sucursal', 'IdSucursal';
EXEC actualizar_id_de_dup_nd 'tmp_venta', 'tmp_producto', 'IdProducto';


-- Se agrega la descripcion del canal de venta
UPDATE v
SET v.Tipo_Canal_Venta = cv.Descripcion
FROM tmp_venta v JOIN tmp_canal_venta cv ON v.IdCanal = cv.IdCanal;

-- Se aplica nuevo ID para IDs Desconocido
EXEC actualizar_ids_desconocidos 'tmp_venta', 'IdVenta'


-- Se solucionan posibles casos resultantes donde el precio del producto es erroneo y no fue solucionado.
UPDATE v
SET v.Precio = p.Precio, v.Compra_Total = p.Precio * v.Cantidad
FROM 
    tmp_venta v 
INNER JOIN 
    tmp_producto p ON v.IdProducto = p.IdProducto
WHERE 
    v.Precio = -1 AND p.Producto_Descartado=0;

-- Se normaliza cualquier caso donde Compra_Total sea menor que -1
UPDATE tmp_venta
SET Compra_Total = 0 -- Anteriormente se utilizaba el valor -1 pero para evitar problemas ahora se utiliza 0
WHERE Compra_Total <= -1;

-- Se almacenan los errores

-- (1) IdVenta
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_IdVenta' AS Columna,
	IdVenta AS IdPrincipal,
	IdVenta AS Valor_Almacenado,
	Error_IdVenta AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_IdVenta != 'Desconocido';

-- (2) IdCanal
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_IdCanal' AS Columna,
	IdVenta AS IdPrincipal,
	IdCanal AS Valor_Almacenado,
	Error_IdCanal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_IdCanal != 'Desconocido';

-- (3) Cantidad
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_Cantidad' AS Columna,
	IdVenta AS IdPrincipal,
	Cantidad AS Valor_Almacenado,
	Error_Cantidad AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_Cantidad != 'Desconocido';

-- (4) Precio
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_Precio' AS Columna,
	IdVenta AS IdPrincipal,
	Precio AS Valor_Almacenado,
	Error_Precio AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_Precio != 'Desconocido' AND CAST(Precio AS NVARCHAR(255))!=Error_Precio AND Outlier=0;

-- (5) Fecha_Venta
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_Fecha_Venta' AS Columna,
	IdVenta AS IdPrincipal,
	Fecha_Venta AS Valor_Almacenado,
	Error_Fecha_Venta AS Valor_Erroneo,
	GETDATE() AS Fecha_Venta_Carga
FROM
	tmp_venta
WHERE Error_Fecha_Venta != 'Desconocido';

-- (6) Fecha_Entrega
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_Fecha_Entrega' AS Columna,
	IdVenta AS IdPrincipal,
	Fecha_Entrega AS Valor_Almacenado,
	Error_Fecha_Entrega AS Valor_Erroneo,
	GETDATE() AS Fecha_Entrega_Carga
FROM
	tmp_venta
WHERE Error_Fecha_Entrega != 'Desconocido';


-- (7) IdCliente
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_IdCliente' AS Columna,
	IdVenta AS IdPrincipal,
	IdCliente AS Valor_Almacenado,
	Error_IdCliente AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_IdCliente != 'Desconocido';

-- (8) IdSucursal
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_IdSucursal' AS Columna,
	IdVenta AS IdPrincipal,
	IdSucursal AS Valor_Almacenado,
	Error_IdSucursal AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_IdSucursal != 'Desconocido';

-- (9) IdEmpleado
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'Error_IdEmpleado' AS Columna,
	IdVenta AS IdPrincipal,
	IdEmpleado AS Valor_Almacenado,
	Error_IdEmpleado AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_IdEmpleado != 'Desconocido';

-- (10) Outlier
INSERT INTO tbl_errores(Tabla, Columna, IdPrincipal, Valor_Almacenado, Valor_Erroneo, Fecha_Carga) 
SELECT 
	'tmp_venta' AS Tabla,
	'OUTLIER' AS Columna,
	IdVenta AS IdPrincipal,
	Precio AS Valor_Almacenado,
	Error_Precio AS Valor_Erroneo,
	GETDATE() AS Fecha_Carga
FROM
	tmp_venta
WHERE Error_Precio != 'Desconocido' AND Outlier=1;

