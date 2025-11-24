USE proyecto_matias_alvarez;
GO

/*########################################################################
 ######################### TABLAS AUXILIARES ############################
 ########################################################################*/

 ----------------------------------------------------------
-- Tabla para almacenar los máximos ID de cada tabla
----------------------------------------------------------
/*
 Cuando llega una nuevo lote de datos:
	Si se tiene ID desconocidos en la tabla temporal:
		A) Se busca max_ids en la tabla y se busca el MAX(Id{TMP}) de la tabla
		B) Se elige el mayor valor entre los dos y se aumenta en 1 (MAX(ID) + 1) y se almacena en una variable
		C) Se actualiza cada registro y a su vez se aumenta en 1 la variable

Nota:
	1) Antes de cargar la primera vez debe procesarse cada tabla temporal
	2) En caso de no tener valores en la tabla y tener IDs desconocidos en una tabla temporal debe utilizarse el MAX(ID{TMP}) + 1 de la tabla temporal.
*/


CREATE TABLE max_ids(
	IdTipoProducto INT NOT NULL DEFAULT -1,
	IdProducto INT NOT NULL DEFAULT -1,
	IdCanal INT NOT NULL DEFAULT -1,
	IdTipoGasto INT NOT NULL DEFAULT -1,
	IdProveedor INT NOT NULL DEFAULT -1,
	IdSucursal INT NOT NULL DEFAULT -1,
	IdCliente INT NOT NULL DEFAULT -1,
	IdEmpleado INT NOT NULL DEFAULT -1,
	IdGasto INT NOT NULL DEFAULT -1,
	IdCompra INT NOT NULL DEFAULT -1,
	IdVenta INT NOT NULL DEFAULT -1
);

INSERT INTO max_ids (IdTipoProducto, IdProducto, IdCanal, IdTipoGasto, IdProveedor, IdSucursal, IdCliente, IdEmpleado, IdGasto, IdCompra, IdVenta)
VALUES (-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1);


 ----------------------------------------------------------
-- Tabla de Feriados
----------------------------------------------------------
-- Tabla temporal de feriados, utilizada para cargar todo el CSV
CREATE TABLE #tmp_feriados(
   Fecha NVARCHAR(11),
   Dia NVARCHAR(11),
   Conmemoracion NVARCHAR(255),
   Fecha_Dia INT,
   Fecha_Mes INT,
   Fecha_Anio INT,
   Fecha_MDA NVARCHAR(11),
   Fecha_DT DATE
 );
 
-- Tabla final que contendrá las columnas necesarias
 CREATE TABLE feriados(
   Fecha DATE PRIMARY KEY,
   Fecha_Dia INT,
   Fecha_Mes INT,
   Fecha_Anio INT,
   Conmemoracion NVARCHAR(255),
   Nombre_dia NVARCHAR(11),
 );

-- Se carga el CSV a la tabla temporal
BULK INSERT #tmp_feriados FROM '/home/data/Feriados.csv' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2);
DELETE FROM #tmp_feriados WHERE Fecha_DT = '2016-03-24' AND Conmemoracion = 'Desconocido'; -- HAY un dup

-- Se cargan los datos en la tabla feriados
INSERT INTO feriados(Fecha, Fecha_Dia, Fecha_Mes, Fecha_Anio, Conmemoracion, Nombre_Dia)
SELECT Fecha_DT, DAY(Fecha_DT), MONTH(Fecha_DT), YEAR(Fecha_DT), Conmemoracion, Dia FROM #tmp_feriados;

-- Se elimina la tabla temporal feriados
DROP TABLE #tmp_feriados;

 ----------------------------------------------------------
-- Tabla para almacenar errores
----------------------------------------------------------
-- DROP TABLE tbl_errores;
CREATE TABLE tbl_errores(
	IdError INT IDENTITY(1,1),
	Tabla NVARCHAR(25),
	Columna NVARCHAR(25),
	IdPrincipal INT,
	Valor_Almacenado NVARCHAR(255),
	Valor_Erroneo NVARCHAR(255),
	Fecha_Carga DATETIME
);

/*########################################################################
 ######################### ÁREA DE PREPARACIÓN ###########################
 ########################################################################*/

----------------------------------------------------------
-- (temporal) Producto
----------------------------------------------------------
CREATE TABLE tmp_tipo_producto(
  IdTipoProducto INT,
  TipoProducto NVARCHAR(255),
  IdTipoProducto_Actualizado BIT,
  IdTipoProducto_Descartado BIT DEFAULT 0, 
  IdTipoProducto_Correcto INT DEFAULT -1,
  Error_IdTipoProducto NVARCHAR(255),
  Error_TipoProducto NVARCHAR(255),
  TipoProducto_Descartado NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Producto
----------------------------------------------------------
CREATE TABLE tmp_producto(
  IdProducto INT,
  Producto NVARCHAR(255),
  TipoProducto NVARCHAR(255),
  Marca NVARCHAR(255),
  Modelo NVARCHAR(255),
  Componente NVARCHAR(255),
  Precio FLOAT, 
  IdTipoProducto INT,
  Producto_Descartado BIT,--INT,
  IdProducto_Actualizado BIT,
  IdProducto_Descartado BIT DEFAULT 0, 
  IdProducto_Correcto INT DEFAULT -1,
  Error_IdProducto NVARCHAR(255),
  Error_Precio  NVARCHAR(255),
  Error_IdTipoProducto NVARCHAR(255),
  TipoProducto_Descartado BIT,
  Error_TipoProducto NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Canal de Venta
----------------------------------------------------------
CREATE TABLE tmp_canal_venta(
  IdCanal INT,
  Descripcion NVARCHAR(255),
  IdCanal_Actualizado BIT,
  IdCanal_Descartado BIT DEFAULT 0, 
  IdCanal_Correcto INT DEFAULT -1,
  Error_IdCanal NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Tipo Gasto
----------------------------------------------------------
CREATE TABLE tmp_tipo_gasto(
  IdTipoGasto INT,
  Descripcion NVARCHAR(255),
  Monto_Aproximado NVARCHAR(255),
  IdTipoGasto_Actualizado BIT,
  IdTipoGasto_Descartado BIT DEFAULT 0, 
  IdTipoGasto_Correcto INT DEFAULT -1,
  Error_IdTipoGasto NVARCHAR(255),
  Error_Descripcion NVARCHAR(255),
  Error_Monto NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Proveedor
----------------------------------------------------------
CREATE TABLE tmp_proveedor(
  IdProveedor INT,
  Nombre NVARCHAR(255),
  Tipo_Sociedad NVARCHAR(255),
  Sociedad_Completa NVARCHAR(255),
  Direccion NVARCHAR(255),
  Ciudad NVARCHAR(255),
  Provincia NVARCHAR(255),
  Pais NVARCHAR(255),
  Partido NVARCHAR(255),
  IdProveedor_Actualizado BIT,
  IdProveedor_Descartado BIT DEFAULT 0, 
  IdProveedor_Correcto INT DEFAULT -1,
  Error_IdProveedor NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Sucursal
----------------------------------------------------------
CREATE TABLE tmp_sucursal(
  IdSucursal INT,
  Sucursal_Completa NVARCHAR(255),
  Sucursal NVARCHAR(255),
  Numero_Sucursal INT,
  Direccion NVARCHAR(255),
  Localidad NVARCHAR(255),
  Provincia NVARCHAR(255),
  Latitud NVARCHAR(15),
  Longitud NVARCHAR(15),
  IdSucursal_Actualizado BIT,
  IdSucursal_Descartado BIT DEFAULT 0, 
  IdSucursal_Correcto INT DEFAULT -1,
  Error_Latitud NVARCHAR(255),
  Error_Longitud NVARCHAR(255),
  Error_IdSucursal NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Cliente
----------------------------------------------------------
CREATE TABLE tmp_cliente(
  IdCliente INT,
  Provincia NVARCHAR(255),
  Nombre_y_Apellido NVARCHAR(255),
  Nombre_Completo NVARCHAR(255),
  Apellido_Completo NVARCHAR(255),
  Nombre_1 NVARCHAR(255),
  Nombre_2 NVARCHAR(255),
  Nombre_3 NVARCHAR(255),
  Apellido_1 NVARCHAR(255),
  Apellido_2 NVARCHAR(255),
  Apellido_3 NVARCHAR(255),
  Domicilio NVARCHAR(255),
  Edad INT,
  Grupo_Etario NVARCHAR(12),
  Mayor_Edad INT,
  Localidad NVARCHAR(255),
  Latitud NVARCHAR(15),
  Longitud NVARCHAR(15),
  IdCliente_Actualizado BIT,
  IdCliente_Descartado BIT DEFAULT 0, 
  IdCliente_Correcto INT DEFAULT -1,
  Error_IdCliente NVARCHAR(255),
  Error_Edad NVARCHAR(255),
  Error_Longitud NVARCHAR(255),
  Error_Latitud NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Empleado
----------------------------------------------------------
CREATE TABLE tmp_empleado(
  IdEmpleado INT,
  Nombre_y_Apellido NVARCHAR(255),
  Apellido NVARCHAR(255),
  Nombre NVARCHAR(255),
  Sucursal NVARCHAR(255),
  Numero_Sucursal TINYINT DEFAULT -1,
  Sucursal_Completa NVARCHAR(255) DEFAULT 'Desconocido',
  Sector NVARCHAR(255),
  Cargo NVARCHAR(255),
  Salario NVARCHAR(255),
  IdEmpleado_Actualizado BIT,
  IdEmpleado_Descartado BIT DEFAULT 0, 
  IdEmpleado_Correcto INT DEFAULT -1,
  Error_IdEmpleado NVARCHAR(255),
  Error_Sucursal NVARCHAR(255),
  Error_Salario NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Gasto
----------------------------------------------------------
CREATE TABLE tmp_gasto(
  IdGasto INT,
  IdSucursal INT,
  IdTipoGasto INT,
  Descripcion NVARCHAR(255),
  Monto FLOAT,
  Monto_Aproximado_Tipo INT DEFAULT -1,
  IdFecha INT,
  Fecha DATE,
  Fecha_Dia INT,
  Fecha_Mes INT,
  Fecha_Anio INT,
  Fecha_Periodo INT,
  Numero_Dia_Semana INT,
  Nombre_Dia NVARCHAR(11),
  Semana_Anio INT,
  Nombre_Mes NVARCHAR(255),
  Trimestre INT,
  Feriado INT,
  Outlier INT,
  Error_IdGasto NVARCHAR(255),
  Error_IdSucursal NVARCHAR(255),
  Error_IdTipoGasto NVARCHAR(255),
  Error_Monto NVARCHAR(255),
  Error_Fecha NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Compra
----------------------------------------------------------
CREATE TABLE tmp_compra(
  IdCompra INT,
  IdFecha INT,
  Fecha DATE,
  Fecha_Anio INT,
  Fecha_Mes INT,
  Fecha_Dia INT,
  Fecha_Periodo INT,
  Numero_Dia_Semana INT,
  Nombre_Dia NVARCHAR(11),
  Semana_Anio INT,
  Nombre_Mes NVARCHAR(11),
  Trimestre INT,
  Feriado INT,
  Outlier INT,
  IdProducto INT,
  Cantidad INT,
  Precio FLOAT, 
  Compra_Total FLOAT DEFAULT -1,
  IdProveedor INT,
  Error_IdCompra NVARCHAR(255),
  Error_IdProducto NVARCHAR(255),
  Error_Cantidad NVARCHAR(255),
  Error_Precio NVARCHAR(255),
  Error_Fecha NVARCHAR(255),
  Error_IdProveedor NVARCHAR(255)
);

----------------------------------------------------------
-- (temporal) Venta
----------------------------------------------------------
CREATE TABLE tmp_venta(
  IdVenta INT,
  Fecha_Venta DATE,
  Fecha_Entrega DATE,
  Dias_Entregado INT,
  IdCanal INT,
  IdCliente INT,
  IdSucursal INT,
  IdEmpleado INT,
  IdProducto INT,
  Precio FLOAT,
  Cantidad INT,
  Compra_Total DECIMAL(10,2),
  Outlier INT,
  Tipo_Canal_Venta NVARCHAR(11),
  IdFecha_Venta INT,
  Fecha_Dia_Venta INT,
  Fecha_Mes_Venta INT,
  Nombre_Mes_Venta NVARCHAR(255),
  Fecha_Anio_Venta INT,
  Fecha_Periodo_Venta INT,
  Numero_Dia_Semana_Venta INT,
  Nombre_Dia_Venta NVARCHAR(255), 
  Semana_Anio_Venta INT,
  Trimestre_Venta INT,
  Feriado_Venta INT,
  IdFecha_Entrega INT,
  Fecha_Dia_Entrega INT,
  Fecha_Mes_Entrega INT,
  Nombre_Mes_Entrega NVARCHAR(255),
  Fecha_Anio_Entrega INT,
  Fecha_Periodo_Entrega INT,
  Numero_Dia_Semana_Entrega INT,
  Nombre_Dia_Entrega NVARCHAR(255),
  Semana_Anio_Entrega INT,
  Trimestre_Entrega INT,
  Feriado_Entrega INT,
  Error_IdVenta NVARCHAR(255),
  Error_Precio NVARCHAR(255),
  Error_IdCanal NVARCHAR(255),
  Error_IdCliente NVARCHAR(255),
  Error_IdSucursal NVARCHAR(255),
  Error_IdEmpleado NVARCHAR(255),
  Error_IdProducto NVARCHAR(255),
  Error_Cantidad NVARCHAR(255),
  Error_Fecha_Venta NVARCHAR(255),
  Error_Fecha_Entrega NVARCHAR(255)
);

/*########################################################################
 ########################## MODELO DIMENSIONAL ###########################
 ########################################################################*/

----------------------------------------------------------
-- Dimension Fecha
----------------------------------------------------------
CREATE TABLE Dim_Fecha (
    IdFecha INT PRIMARY KEY,  -- AAAAMMDD
    Fecha DATE,
    Dia SMALLINT DEFAULT -1, --TINYINT no puede ser negativo
    Mes SMALLINT DEFAULT -1,
    Anio INT,
    Nombre_Dia NVARCHAR(11) DEFAULT 'Desconocido',
	Numero_Dia_Semana NVARCHAR(11) DEFAULT 'Desconocido',
    Nombre_Mes NVARCHAR(11) DEFAULT 'Desconocido',
    Num_Semana_Anio SMALLINT DEFAULT -1,
    Periodo INT DEFAULT 999912,
    Trimestre SMALLINT DEFAULT -1,
    Feriado BIT DEFAULT 0 
);

----------------------------------------------------------
-- Dimension Empleado
----------------------------------------------------------
CREATE TABLE Dim_Empleado (
    EmpleadoSK INT PRIMARY KEY IDENTITY(1,1), 
    IdEmpleado INT,
    Nombre_y_Apellido NVARCHAR(255) DEFAULT 'Desconocido',
    Apellido NVARCHAR(125) DEFAULT 'Desconocido',
    Nombre NVARCHAR(130) DEFAULT 'Desconocido',
    Sucursal_Completa NVARCHAR(255) DEFAULT 'Desconocido',
    Sucursal NVARCHAR(255) DEFAULT 'Desconocido',
    Numero_Sucursal TINYINT DEFAULT -1,
    Sector NVARCHAR(125) DEFAULT 'Desconocido',
    Cargo NVARCHAR(125) DEFAULT 'Desconocido',
    Salario FLOAT DEFAULT -1,
    Sigue_Trabajando BIT NOT NULL,
    Alta DATETIME NOT NULL,
    Baja DATETIME DEFAULT '9999-12-31 23:59:59',
    Version_Actual BIT NOT NULL
);

----------------------------------------------------------
-- Dimension Sucursal
----------------------------------------------------------
CREATE TABLE Dim_Sucursal (
    SucursalSK INT PRIMARY KEY IDENTITY(1,1), 
    IdSucursal INT,
    Sucursal NVARCHAR(255) DEFAULT 'Desconocido',
    Numero_Sucursal TINYINT DEFAULT -1,
    Sucursal_Completa NVARCHAR(255) DEFAULT 'Desconocido',
    Direccion NVARCHAR(255) DEFAULT 'Desconocido',
    Localidad NVARCHAR(255) DEFAULT 'Desconocido',
    Provincia NVARCHAR(255) DEFAULT 'Desconocido',
    Latitud NVARCHAR(12) DEFAULT '-1',
    Longitud NVARCHAR(12) DEFAULT '-1',
    Alta DATETIME NOT NULL,
    Baja DATETIME DEFAULT '9999-12-31 23:59:59',
    Version_Actual BIT NOT NULL
);

----------------------------------------------------------
-- Dimension Producto
----------------------------------------------------------
CREATE TABLE Dim_Producto (
    ProductoSK INT PRIMARY KEY IDENTITY(1,1), 
    IdProducto INT NOT NULL,
    IdTipoProducto INT NOT NULL,
    Producto NVARCHAR(255) DEFAULT 'Desconocido',
    TipoProducto NVARCHAR(255) DEFAULT 'Desconocido',
    Marca NVARCHAR(255),
    Modelo NVARCHAR(255),
    Componente NVARCHAR(255),
    Precio FLOAT CHECK (Precio >= -1),
    Producto_Descartado BIT NOT NULL,
    Alta DATETIME NOT NULL,
    Baja DATETIME DEFAULT '9999-12-31 23:59:59',
    Version_Actual BIT NOT NULL
);

----------------------------------------------------------
-- Dimension Cliente
----------------------------------------------------------
CREATE TABLE Dim_Cliente (
    ClienteSK INT PRIMARY KEY IDENTITY(1,1), 
    IdCliente INT,
    Nombre_y_Apellido NVARCHAR(255) DEFAULT 'Desconocido',
    Nombre_Completo NVARCHAR(255),
    Apellido_Completo NVARCHAR(255),
    Nombre_1 NVARCHAR(255),
    Nombre_2 NVARCHAR(255),
    Nombre_3 NVARCHAR(255),
    Apellido_1 NVARCHAR(255),
    Apellido_2 NVARCHAR(255),
    Apellido_3 NVARCHAR(255),
    Edad INT DEFAULT -1 CHECK (Edad >= -1),
    Grupo_Etario NVARCHAR(12) DEFAULT 'Desconocido',
    Mayor_Edad BIT,
    Domicilio NVARCHAR(255) DEFAULT 'Desconocido',
    Provincia NVARCHAR(255) DEFAULT 'Desconocido',
    Localidad NVARCHAR(255) DEFAULT 'Desconocido',
    Latitud NVARCHAR(12) DEFAULT '-1',
    Longitud NVARCHAR(12) DEFAULT '-1',
    Alta DATETIME NOT NULL,
    Baja DATETIME DEFAULT '9999-12-31 23:59:59',
    Version_Actual BIT NOT NULL
);

----------------------------------------------------------
-- Dimension Proveedor
----------------------------------------------------------

CREATE TABLE Dim_Proveedor (
    ProveedorSK INT PRIMARY KEY IDENTITY(1,1), 
    IdProveedor INT,
    Sociedad_Completa NVARCHAR(100) DEFAULT 'Desconocido',
    Nombre NVARCHAR(255) DEFAULT 'Desconocido',
    Tipo_Sociedad NVARCHAR(11) DEFAULT 'Desconocido',
    Direccion NVARCHAR(255) DEFAULT 'Desconocido',
    Ciudad NVARCHAR(255) DEFAULT 'Desconocido',
    Provincia NVARCHAR(255) DEFAULT 'Desconocido',
    Pais NVARCHAR(255) DEFAULT 'Desconocido',
    Departamento NVARCHAR(255) DEFAULT 'Desconocido',
    Alta DATETIME NOT NULL,
    Baja DATETIME DEFAULT '9999-12-31 23:59:59',
    Version_Actual BIT NOT NULL
);

----------------------------------------------------------
-- Hecho Ventas
----------------------------------------------------------

CREATE TABLE Fact_Ventas (
    VentaSK INT PRIMARY KEY IDENTITY(1,1), 
    IdVenta INT,
    ClienteSK INT,
    SucursalSK INT,
    EmpleadoSK INT,
    ProductoSK INT,
    IdFecha_Venta INT,
    IdFecha_Entrega INT,
    Dias_Entregado INT DEFAULT -1,
    Precio  FLOAT DEFAULT -1 CHECK (Precio >= -1), 
    Cantidad INT DEFAULT 1 CHECK (Cantidad >= 1),
    Compra_Total FLOAT DEFAULT -1 ,
    Outlier BIT,
	IdCanal INT,
    Tipo_Canal NVARCHAR(50) DEFAULT 'Desconocido',
    FOREIGN KEY (ClienteSK) REFERENCES Dim_Cliente(ClienteSK),
    FOREIGN KEY (SucursalSK) REFERENCES Dim_Sucursal(SucursalSK),
    FOREIGN KEY (EmpleadoSK) REFERENCES Dim_Empleado(EmpleadoSK),
    FOREIGN KEY (ProductoSK) REFERENCES Dim_Producto(ProductoSK),
    FOREIGN KEY (IdFecha_Venta) REFERENCES Dim_Fecha(IdFecha),
    FOREIGN KEY (IdFecha_Entrega) REFERENCES Dim_Fecha(IdFecha)
);

----------------------------------------------------------
-- Hecho Compras
----------------------------------------------------------
CREATE TABLE Fact_Compras (
    CompraSK INT PRIMARY KEY IDENTITY(1,1), 
    IdCompra INT,
    ProductoSK INT,
    ProveedorSK INT,
    IdFecha INT,
    Cantidad INT DEFAULT 1 CHECK (Cantidad >= 1), -- Como mínimo solo puede haber una cantidad de 1
    Precio FLOAT DEFAULT -1 CHECK (Precio >= -1),
    Compra_Total FLOAT DEFAULT -1, 
    Outlier BIT,  
    FOREIGN KEY (ProductoSK) REFERENCES Dim_Producto(ProductoSK),
    FOREIGN KEY (ProveedorSK) REFERENCES Dim_Proveedor(ProveedorSK),
    FOREIGN KEY (IdFecha) REFERENCES Dim_Fecha(IdFecha)
);

----------------------------------------------------------
-- Hecho Gasto
----------------------------------------------------------

CREATE TABLE Fact_Gastos (
    GastoSK INT PRIMARY KEY IDENTITY(1,1), 
    IdGasto INT,
    SucursalSK INT,
    IdFecha INT,
	IdTipoGasto INT,
    TipoGasto NVARCHAR(50) DEFAULT 'Desconocido',
    Monto FLOAT DEFAULT -1 CHECK (Monto >= -1), 
    Outlier BIT,
    FOREIGN KEY (SucursalSK) REFERENCES Dim_Sucursal(SucursalSK),
    FOREIGN KEY (IdFecha) REFERENCES Dim_Fecha(IdFecha)
);

-- SELECT * FROM tmp_tipo_producto; 
-- SELECT * FROM tmp_producto; 
-- SELECT * FROM tmp_canal_venta; 
-- SELECT * FROM tmp_tipo_gasto; 
-- SELECT * FROM tmp_proveedor;
-- SELECT * FROM tmp_sucursal; 
-- SELECT * FROM tmp_cliente; 
-- SELECT * FROM tmp_empleado;
-- SELECT * FROM tmp_gasto; 
-- SELECT * FROM tmp_compra; 
-- SELECT * FROM tmp_venta; 

 
-- SELECT * FROM dim_producto; 
-- SELECT * FROM dim_proveedor;
-- SELECT * FROM dim_sucursal; 
-- SELECT * FROM dim_cliente; 
-- SELECT * FROM dim_empleado;
-- SELECT * FROM Fact_Gastos; 
-- SELECT * FROM fact_compras; 
-- SELECT * FROM fact_ventas; 
