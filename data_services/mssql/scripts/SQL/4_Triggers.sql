/*
Creación de triggers para:
	+ Manejar los casos de SCD tipo 2 en las dimensiones (a excepción de Dim_Fecha)
	+ Se actualiza tras cada inserción si es necesario los ID máximo existentes en la base de datos en la tabla max_ids
*/

use proyecto_matias_alvarez;
GO

----------------------------------------------------------
-- Dim_Empleado
----------------------------------------------------------
CREATE TRIGGER trg_ins_Dim_Empleado
	ON Dim_Empleado
	AFTER INSERT
	AS
BEGIN
    -- Se Verifica si el IdEmpleado insertado es mayor que el máximo valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdEmpleado > m.IdEmpleado)
    BEGIN
        -- En caso afirmativo se actualiza el valor en max_ids con el nuevo IdEmpleado
        UPDATE max_ids
        SET IdEmpleado = (SELECT MAX(i.IdEmpleado) FROM inserted i);
    END

    -- Se verifica si el IdEmpleado ya existe como activo
    IF EXISTS (SELECT 1 
               FROM Dim_Empleado de
               JOIN inserted i ON de.IdEmpleado = i.IdEmpleado
               WHERE 
					de.IdEmpleado = i.IdEmpleado AND
					de.Version_Actual = 1 AND
					de.Alta < i.Alta
				)
    BEGIN
        -- Se actualiza el registro existente para marcarlo como inactivo
        UPDATE Dim_Empleado
        SET Version_Actual = 0, 
            Baja = GETDATE()
		FROM Dim_Empleado de 
		JOIN inserted i ON de.IdEmpleado = i.IdEmpleado
        WHERE
			de.Version_Actual = 1 AND 
			de.Alta < i.Alta;
    END
END;
GO

----------------------------------------------------------
-- Dim_Producto
----------------------------------------------------------
CREATE TRIGGER trg_ins_dim_producto
    ON Dim_Producto
    FOR INSERT
AS
BEGIN
    -- Se verifica si el IdProducto insertado es mayor que el máximo valor en max_ids
    IF EXISTS (SELECT 1 
               FROM inserted i
               WHERE i.IdProducto > (SELECT MAX(IdProducto) FROM max_ids))
    BEGIN
        -- Se actualiza el valor en max_ids con el nuevo IdProducto
        UPDATE max_ids
        SET IdProducto = (SELECT MAX(i.IdProducto) FROM inserted i);
    END

    -- Se verifica si el IdTipoProducto insertado es mayor que el máximo valor en max_ids
    IF EXISTS (SELECT 1 
               FROM inserted i
               WHERE i.IdTipoProducto > (SELECT MAX(IdTipoProducto) FROM max_ids))
    BEGIN
        -- Se actualiza el valor en max_ids con el nuevo IdTipoProducto
        UPDATE max_ids
        SET IdTipoProducto = (SELECT MAX(i.IdTipoProducto) FROM inserted i);
    END

    -- Se verifica si el IdProducto ya existe como activo (es decir, versión 1)
    IF EXISTS (SELECT 1 
                   FROM Dim_Producto dp
                   INNER JOIN inserted i ON dp.IdProducto = i.IdProducto
                   WHERE 
				    dp.IdProducto = i.IdProducto AND
					dp.Version_Actual = 1 AND 
					dp.Alta < i.Alta	
				)

    BEGIN
        -- Se actualiza el registro existente para marcarlo como inactivo
        UPDATE Dim_Producto
        SET Version_Actual = 0, 
            Baja = GETDATE()
        FROM Dim_Producto dp
        INNER JOIN inserted i ON dp.IdProducto = i.IdProducto
        WHERE dp.Version_Actual = 1 AND  dp.Alta < i.Alta;
    END

END;
GO

----------------------------------------------------------
-- Dim_Proveedor
----------------------------------------------------------
CREATE TRIGGER trg_ins_dim_proveedor
	ON Dim_Proveedor
	FOR INSERT
	AS
BEGIN
	-- Se verifica si el IdProveedor insertado es mayor que el máximo valor en max_ids
	IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdProveedor > m.IdProveedor)
	BEGIN
		-- Se actualiza el valor en max_ids con el nuevo IdProveedor
		UPDATE max_ids
		SET IdProveedor = (SELECT MAX(i.IdProveedor) FROM inserted i);
	END

	-- Se verifica si el IdProveedor ya existe como activo
	IF EXISTS (SELECT 1 
				FROM Dim_Proveedor dp
				JOIN inserted i ON dp.IdProveedor = i.IdProveedor
				WHERE
					dp.IdProveedor = i.IdProveedor AND
					dp.Version_Actual = 1 AND
					dp.Alta < i.Alta
				)
	BEGIN
		-- Se actualiza el registro existente para marcarlo como inactivo
		UPDATE Dim_Proveedor
		SET Version_Actual = 0, 
			Baja = GETDATE()
		FROM Dim_Proveedor dp 
		JOIN inserted i  ON dp.IdProveedor = i.IdProveedor
		WHERE 
			dp.Alta < i.Alta AND
			dp.Version_Actual = 1;
	END
END;
GO

----------------------------------------------------------
-- Dim_Sucursal
----------------------------------------------------------
-- DROP TRIGGER trg_ins_dim_sucursal;

CREATE TRIGGER trg_ins_dim_sucursal
	ON Dim_Sucursal
	FOR INSERT
	AS
BEGIN
    -- Se verifica si el IdSucursal insertado es mayor que el máximo valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdSucursal > m.IdSucursal)
    BEGIN
        -- Se actualiza el valor en max_ids con el nuevo IdSucursal
        UPDATE max_ids
        SET IdSucursal = (SELECT MAX(i.IdSucursal) FROM inserted i);
    END

    -- Se verifica si el IdSucursal ya existe como activo
    IF EXISTS (SELECT 1 
               FROM Dim_Sucursal ds
               JOIN inserted i ON ds.IdSucursal = i.IdSucursal
               WHERE
					ds.IdSucursal = i.IdSucursal AND
					ds.Version_Actual = 1 ANd
					ds.Alta < i.Alta)
    BEGIN
        -- Se actualiza el registro existente para marcarlo como inactivo
        UPDATE Dim_Sucursal
        SET Version_Actual = 0, 
            Baja = GETDATE()
		FROM Dim_Sucursal ds
		JOIN inserted i ON ds.IdSucursal = i.IdSucursal 
        WHERE
			ds.Version_Actual = 1 AND
			ds.Alta < i.Alta;
    END
END;
GO

----------------------------------------------------------
-- Dim_Cliente
----------------------------------------------------------
-- DROP TRIGGER trg_ins_Dim_Cliente;

CREATE TRIGGER trg_ins_Dim_Cliente
	ON Dim_Cliente
	AFTER INSERT
	AS
BEGIN
    -- Se verifica si el IdCliente insertado es mayor que el máximo valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdCliente > m.IdCliente)
    BEGIN
        -- Se actualiza el valor en max_ids con el nuevo IdCliente
        UPDATE max_ids
        SET IdCliente = (SELECT MAX(i.IdCliente) FROM inserted i);
    END

    -- Se verifica si el IdCliente ya existe como activo
    IF EXISTS (SELECT 1 
               FROM Dim_Cliente dc
               JOIN inserted i ON dc.IdCliente = i.IdCliente
               WHERE
					dc.IdCliente = i.IdCliente AND
					dc.Version_Actual = 1 AND
					dc.Alta < i.Alta	
				)
    BEGIN
        -- Se actualiza el registro existente para marcarlo como inactivo
        UPDATE Dim_Cliente
        SET Version_Actual = 0, 
            Baja = GETDATE()
        FROM Dim_Cliente dc
        JOIN inserted i ON dc.IdCliente = i.IdCliente
        WHERE 
			dc.Version_Actual = 1 AND
			dc.Alta < i.Alta;
    END
END;
GO

----------------------------------------------------------
-- Fact_Gastos
----------------------------------------------------------
CREATE TRIGGER trg_ins_fact_gastos
	ON Fact_Gastos
	FOR INSERT
	AS
BEGIN
    -- Se verifica si el IdGasto insertado es mayor que el valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdGasto > m.IdGasto)
    BEGIN
        -- Se actualiza max_ids con el nuevo IdGasto
        UPDATE max_ids
        SET IdGasto = (SELECT MAX(i.IdGasto) FROM inserted i);
    END

    -- Se verifica si el IdTipoGasto insertado es mayor que el valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdTipoGasto > m.IdTipoGasto)
    BEGIN
        -- Se actualiza max_ids con el nuevo IdTipoGasto
        UPDATE max_ids
        SET IdTipoGasto = (SELECT MAX(i.IdTipoGasto) FROM inserted i);
    END
END;
GO

----------------------------------------------------------
-- Fact_Ventas
----------------------------------------------------------
-- DROP TRIGGER trg_ins_ventas
CREATE TRIGGER trg_ins_ventas
	ON Fact_Ventas
	FOR INSERT
	AS
BEGIN
    -- Se verifica si el IdVenta insertado es mayor que el valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdVenta > m.IdVenta)
    BEGIN
        -- Se actualiza max_ids con el nuevo IdVenta
        UPDATE max_ids
        SET IdVenta = (SELECT MAX(i.IdVenta) FROM inserted i);
    END

    -- Se verifica si el IdCanal insertado es mayor que el valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdCanal > m.IdCanal)
    BEGIN
        -- Se actualiza max_ids con el nuevo IdCanal
        UPDATE max_ids
        SET IdCanal = (SELECT MAX(i.IdCanal) FROM inserted i);
    END

END;
GO

----------------------------------------------------------
-- Fact_Compras
----------------------------------------------------------
CREATE TRIGGER trg_ins_fact_compras
	ON Fact_Compras
	FOR INSERT
	AS
BEGIN
    -- Se verifica si el IdCompra insertado es mayor que el valor en max_ids
    IF EXISTS (SELECT 1 FROM inserted i
               JOIN max_ids m ON i.IdCompra > m.IdCompra)
    BEGIN
        -- Se actualiza max_ids con el nuevo IdCompra
        UPDATE max_ids
        SET IdCompra = (SELECT MAX(i.IdCompra) FROM inserted i);
    END

END;
GO