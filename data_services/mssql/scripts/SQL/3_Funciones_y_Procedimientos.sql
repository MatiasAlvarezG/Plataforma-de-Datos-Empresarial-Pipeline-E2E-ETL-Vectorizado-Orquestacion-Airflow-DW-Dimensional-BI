use proyecto_matias_alvarez;
GO

----------------------------------------------------------
-- Actualiar ID DESCONOCIDOS
----------------------------------------------------------
CREATE PROCEDURE actualizar_ids_desconocidos
    @nombre_tabla NVARCHAR(255),
    @nombre_columna NVARCHAR(255)
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @max_id INT;
    DECLARE @id_actual INT;
    DECLARE @cant_filas INT = 0;
    DECLARE @tabla_max_ids NVARCHAR(255) = 'max_ids';
    DECLARE @nombre_columna_correcto NVARCHAR(255) = CONCAT(@nombre_columna, '_Correcto');
    DECLARE @error_columna NVARCHAR(255) = 'Error_' + @nombre_columna;

    -- Se obtiene el valor máximo entre la columna en max_ids y la columna en la tabla temporal
    SET @sql = N'
        SELECT @max_id = MAX(CASE WHEN a.' + QUOTENAME(@nombre_columna) + ' > b.' + QUOTENAME(@nombre_columna) + ' THEN a.' + QUOTENAME(@nombre_columna) + ' ELSE b.' + QUOTENAME(@nombre_columna) + ' END)
        FROM (SELECT MAX(' + QUOTENAME(@nombre_columna) + ') AS ' + QUOTENAME(@nombre_columna) + ' FROM ' + QUOTENAME(@nombre_tabla) + ') AS a
        CROSS JOIN (SELECT ' + QUOTENAME(@nombre_columna) + ' FROM ' + QUOTENAME(@tabla_max_ids) + ') AS b;';

    EXEC sp_executesql @sql, N'@max_id INT OUTPUT', @max_id = @max_id OUTPUT;

    -- Se Incrementa @max_id para el primer ID desconocido (ya que la tabla max_ids almacena el valor más alto)
    SET @max_id = @max_id + 1;

	SET @sql = N'SELECT @cant_filas = COUNT(*) FROM ' + QUOTENAME(@nombre_tabla) + ' WHERE ' + QUOTENAME(@nombre_columna) + ' = -1';
	EXEC sp_executesql @sql, N'@cant_filas INT OUTPUT', @cant_filas OUTPUT;

    IF @cant_filas = 0
    BEGIN
        PRINT 'No hay registros con ID desconocido (-1). El procedimiento no se ejecutar�.';
        RETURN;
    END

    -- Se crea un cursor para procesar cada fila con ID desconocido (-1) en la tabla temporal
    SET @sql = N'
        DECLARE id_cursor CURSOR FOR
        SELECT ' + QUOTENAME(@nombre_columna) + ' FROM ' + QUOTENAME(@nombre_tabla) + ' WHERE ' + QUOTENAME(@nombre_columna) + ' = -1;

        OPEN id_cursor;
        FETCH NEXT FROM id_cursor INTO @id_actual;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Actualizar el registro actual con el valor de @max_id y luego incrementar @max_id
            UPDATE ' + QUOTENAME(@nombre_tabla) + '
            SET ' + QUOTENAME(@nombre_columna) + ' = @max_id,
                ' + QUOTENAME(@nombre_columna_correcto) + ' = @max_id,
                ' + QUOTENAME(@error_columna) + ' = -1
            WHERE CURRENT OF id_cursor;

            SET @max_id = @max_id + 1;
            -- SET @cant_filas = @cant_filas + 1; -- Incrementar el contador de filas procesadas

            FETCH NEXT FROM id_cursor INTO @id_actual;
        END

        CLOSE id_cursor;
        DEALLOCATE id_cursor;';

    EXEC sp_executesql @sql, N'@max_id INT, @id_actual INT OUTPUT, @cant_filas INT OUTPUT', @max_id = @max_id, @id_actual = @id_actual, @cant_filas = @cant_filas;

    -- Se actualiza el valor máximo en la tabla max_ids
    SET @sql = N'
        UPDATE ' + QUOTENAME(@tabla_max_ids) + '
        SET ' + QUOTENAME(@nombre_columna) + ' = (@max_id - 1) + @cant_filas;';

    EXEC sp_executesql @sql, N'@max_id INT, @cant_filas INT', @max_id = @max_id, @cant_filas = @cant_filas;
END;
GO

----------------------------------------------------------
-- Aplicar Formato Title a una columna especificada que sea una sola palabra
----------------------------------------------------------

CREATE PROCEDURE aplicar_title_una_palabra
    @nombreTabla NVARCHAR(255),
    @nombreColumna NVARCHAR(255)
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);

    SET @sql = 'UPDATE ' + QUOTENAME(@nombreTabla) + 
               ' SET ' + QUOTENAME(@nombreColumna) + ' = UPPER(SUBSTRING(' + QUOTENAME(@nombreColumna) + ', 1, 1)) + SUBSTRING(' + QUOTENAME(@nombreColumna) + ', 2, LEN(' + QUOTENAME(@nombreColumna) + ') - 1);';

    EXEC sp_executesql @sql;
END;
GO

----------------------------------------------------------
-- Aplicar Outlier en tablas temporales 'venta', 'gasto' y 'compra'
----------------------------------------------------------
CREATE PROCEDURE marcar_outliers
AS
BEGIN
    -- Marcar outliers en la tabla tmp_venta
    UPDATE tmp_venta
    SET Outlier = 1
    WHERE Precio > (SELECT AVG(Precio) + 2 * STDEV(Precio)
                    FROM tmp_producto
                    WHERE Producto_Descartado = 0);

    -- Marcar outliers en la tabla tmp_compra
    UPDATE tmp_compra
    SET Outlier = 1
    WHERE Precio > (SELECT AVG(Precio) + 2 * STDEV(Precio)
                    FROM tmp_producto
                    WHERE Producto_Descartado = 0);

    -- Marcar outliers en la tabla tmp_gasto
    UPDATE tmp_gasto
    SET Outlier = 1
    WHERE Monto > (SELECT AVG(CAST(Monto_Aproximado AS FLOAT)) + 2 * STDEV(CAST(Monto_Aproximado AS FLOAT))
                   FROM tmp_tipo_gasto
                   WHERE IdTipoGasto_Descartado = 0);
END;
GO

----------------------------------------------------------
-- Actualizar ID erroneos por duplicación no directa
----------------------------------------------------------
CREATE PROCEDURE actualizar_id_de_dup_nd
    @tabla_hecho NVARCHAR(255),
    @tabla_dim NVARCHAR(255),
    @columna_id NVARCHAR(255)
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @columna_correcta NVARCHAR(255);
    DECLARE @columna_descartada NVARCHAR(255);
    DECLARE @columna_error NVARCHAR(255);

    SET @columna_correcta = @columna_id + '_Correcto';
    SET @columna_descartada = @columna_id + '_Descartado';
    SET @columna_error = 'Error_' + @columna_id;

	SET @sql = '
	WITH id_a_actualizar AS (
		SELECT DISTINCT
			t1.' + QUOTENAME(@columna_id) + ' AS original, 
			t2.' + QUOTENAME(@columna_correcta) + ' AS actualizar
		FROM 
			' + QUOTENAME(@tabla_hecho) + ' t1 
		JOIN ' + QUOTENAME(@tabla_dim) + ' t2 ON t1.' + QUOTENAME(@columna_id) + ' = t2.' + QUOTENAME(@columna_id) + '
		WHERE t2.' + QUOTENAME(@columna_descartada) + ' = 1
	)
	UPDATE t
	SET 
		t.' + QUOTENAME(@columna_error) + ' = act.original,
		t.' + QUOTENAME(@columna_id) + ' = act.actualizar
	FROM ' + QUOTENAME(@tabla_hecho) + ' t 
	JOIN id_a_actualizar act ON t.' + QUOTENAME(@columna_id) + ' = act.original;';

	--PRINT @sql;

    EXEC sp_executesql @sql;
END;
GO

----------------------------------------------------------
-- Procesar Duplicados No Directos
----------------------------------------------------------

---------------------------
-- Tipo Producto
---------------------------

CREATE PROCEDURE procesar_dup_nd_tipo_producto
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdTipoProducto,
				TipoProducto,
				ROW_NUMBER() OVER (PARTITION BY TipoProducto ORDER BY IdTipoProducto) AS rn,
				COUNT(*) OVER (PARTITION BY TipoProducto) AS cnt,
				DENSE_RANK() OVER (ORDER BY TipoProducto) AS Grupo_Perteneciente,
				FIRST_VALUE(IdTipoProducto) OVER (PARTITION BY TipoProducto ORDER BY IdTipoProducto) AS IdOriginal
			FROM 
				tmp_tipo_producto
		), ids_duplicados AS (
		SELECT 
			IdTipoProducto,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE p
		SET 
		  p.IdTipoProducto_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  p.IdTipoProducto_Correcto = dup.IdOriginal
		FROM tmp_tipo_producto p JOIN ids_duplicados dup 
				 ON p.IdTipoProducto = dup.IdTipoProducto;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Producto
---------------------------

CREATE PROCEDURE procesar_dup_nd_producto
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdProducto,
				Producto,
				IdTipoProducto,
				ROW_NUMBER() OVER (PARTITION BY Producto, IdTipoProducto ORDER BY IdProducto) AS rn,
				COUNT(*) OVER (PARTITION BY Producto, IdTipoProducto) AS cnt,
				DENSE_RANK() OVER (ORDER BY Producto, IdTipoProducto) AS Grupo_Perteneciente,
				FIRST_VALUE(IdProducto) OVER (PARTITION BY Producto, IdTipoProducto ORDER BY IdProducto) AS IdOriginal
			FROM 
				tmp_producto
		), ids_duplicados AS (
		SELECT 
			IdProducto,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE p
		SET 
		  p.IdProducto_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  p.IdProducto_Correcto = dup.IdOriginal
		FROM tmp_producto p JOIN ids_duplicados dup 
				 ON p.IdProducto = dup.IdProducto;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Canal Venta
---------------------------

CREATE PROCEDURE procesar_dup_nd_canal_venta
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdCanal,
				Descripcion,
				ROW_NUMBER() OVER (PARTITION BY Descripcion ORDER BY IdCanal) AS rn,
				COUNT(*) OVER (PARTITION BY Descripcion) AS cnt,
				DENSE_RANK() OVER (ORDER BY Descripcion) AS Grupo_Perteneciente,
				FIRST_VALUE(IdCanal) OVER (PARTITION BY Descripcion ORDER BY IdCanal) AS IdOriginal
			FROM 
				tmp_canal_venta
		), ids_duplicados AS (
		SELECT 
			IdCanal,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE cv
		SET 
		  cv.IdCanal_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  cv.IdCanal_Correcto = dup.IdOriginal
		FROM tmp_canal_venta cv JOIN ids_duplicados dup 
				 ON cv.IdCanal = dup.IdCanal;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Tipo Gasto
---------------------------

CREATE PROCEDURE procesar_dup_nd_tipo_gasto
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdTipoGasto,
				Descripcion,
				ROW_NUMBER() OVER (PARTITION BY Descripcion ORDER BY IdTipoGasto) AS rn,
				COUNT(*) OVER (PARTITION BY Descripcion) AS cnt,
				DENSE_RANK() OVER (ORDER BY Descripcion) AS Grupo_Perteneciente,
				FIRST_VALUE(IdTipoGasto) OVER (PARTITION BY Descripcion ORDER BY IdTipoGasto) AS IdOriginal
			FROM 
				tmp_tipo_gasto
		), ids_duplicados AS (
		SELECT 
			IdTipoGasto,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE g
		SET 
		  g.IdTipoGasto_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  g.IdTipoGasto_Correcto = dup.IdOriginal
		FROM tmp_tipo_gasto g JOIN ids_duplicados dup 
				 ON g.IdTipoGasto = dup.IdTipoGasto;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Proveedor
---------------------------

CREATE PROCEDURE procesar_dup_nd_proveedor
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
        WITH grupos_de_duplicados AS (
            SELECT 
                IdProveedor,
                Direccion,
                Ciudad,
                Provincia,
                ROW_NUMBER() OVER (PARTITION BY Direccion, Ciudad, Provincia ORDER BY IdProveedor) AS rn,
                COUNT(*) OVER (PARTITION BY Direccion, Ciudad, Provincia) AS cnt,
                DENSE_RANK() OVER (ORDER BY Direccion, Ciudad, Provincia) AS Grupo_Perteneciente,
                FIRST_VALUE(IdProveedor) OVER (PARTITION BY Direccion, Ciudad, Provincia ORDER BY IdProveedor) AS IdOriginal
            FROM 
                tmp_proveedor
        ), ids_duplicados AS (
            SELECT 
                IdProveedor,
                IdOriginal,
                rn,
                Grupo_Perteneciente
            FROM 
                grupos_de_duplicados 
            WHERE 
                cnt > 1
        )
        UPDATE p
        SET 
            p.IdProveedor_Descartado = CASE 
                WHEN dup.rn > 1 THEN 1 -- No es el primer duplicado del grupo
                ELSE 0 
            END,
            p.IdProveedor_Correcto = dup.IdOriginal
        FROM tmp_proveedor p 
        JOIN ids_duplicados dup ON p.IdProveedor = dup.IdProveedor;

        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Sucursal
---------------------------
CREATE PROCEDURE procesar_dup_nd_sucursal
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdSucursal,
				Sucursal_Completa, 
				Direccion,
				ROW_NUMBER() OVER (PARTITION BY Sucursal_Completa, Direccion ORDER BY IdSucursal) AS rn,
				COUNT(*) OVER (PARTITION BY Sucursal_Completa, Direccion) AS cnt,
				DENSE_RANK() OVER (ORDER BY Sucursal_Completa, Direccion) AS Grupo_Perteneciente,
				FIRST_VALUE(IdSucursal) OVER (PARTITION BY Sucursal_Completa, Direccion ORDER BY IdSucursal) AS IdOriginal
			FROM 
				tmp_sucursal
		), ids_duplicados AS (
		SELECT 
			IdSucursal,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE s
		SET 
		  s.IdSucursal_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  s.IdSucursal_Correcto = dup.IdOriginal
		FROM tmp_sucursal s JOIN ids_duplicados dup 
				 ON s.IdSucursal = dup.IdSucursal;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Cliente
---------------------------

CREATE PROCEDURE procesar_dup_nd_cliente
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdCliente,
				Nombre_y_Apellido,
				Domicilio,
				Grupo_Etario,
				ROW_NUMBER() OVER (PARTITION BY Nombre_y_Apellido, Domicilio, Grupo_Etario ORDER BY IdCliente) AS rn,
				COUNT(*) OVER (PARTITION BY Nombre_y_Apellido, Domicilio, Grupo_Etario) AS cnt,
				DENSE_RANK() OVER (ORDER BY Nombre_y_Apellido, Domicilio, Grupo_Etario) AS Grupo_Perteneciente,
				FIRST_VALUE(IdCliente) OVER (PARTITION BY Nombre_y_Apellido, Domicilio, Grupo_Etario ORDER BY IdCliente) AS IdOriginal
			FROM 
				tmp_cliente
		), ids_duplicados AS (
		SELECT 
			IdCliente,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE c
		SET 
		  c.IdCliente_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  c.IdCliente_Correcto = dup.IdOriginal
		FROM tmp_cliente c JOIN ids_duplicados dup 
				 ON c.IdCliente = dup.IdCliente;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

---------------------------
-- Empleado
---------------------------

CREATE PROCEDURE procesar_dup_nd_empleado
AS
BEGIN
    -- Inicia una transacción para garantizar la atomicidad
    BEGIN TRANSACTION;

    BEGIN TRY
		WITH grupos_de_duplicados AS (
			SELECT 
				IdEmpleado,
				Apellido, 
				Nombre, 
				Sucursal_Completa,
				ROW_NUMBER() OVER (PARTITION BY Apellido, Nombre, Sucursal_Completa ORDER BY IdEmpleado) AS rn,
				COUNT(*) OVER (PARTITION BY Apellido, Nombre, Sucursal_Completa) AS cnt,
				DENSE_RANK() OVER (ORDER BY Apellido, Nombre, Sucursal_Completa) AS Grupo_Perteneciente,
				FIRST_VALUE(IdEmpleado) OVER (PARTITION BY Apellido, Nombre, Sucursal_Completa ORDER BY IdEmpleado) AS IdOriginal
			FROM 
				tmp_empleado
		), ids_duplicados AS (
		SELECT 
			IdEmpleado,
			IdOriginal,
			rn,
			Grupo_Perteneciente
		FROM 
			grupos_de_duplicados 
		WHERE 
			cnt > 1
		)
		UPDATE e
		SET 
		  e.IdEmpleado_Descartado = CASE 
			WHEN dup.rn>1 THEN 1 -- No es el primer duplicado del grupo
			ELSE 0 END,
		  e.IdEmpleado_Correcto = dup.IdOriginal
		FROM tmp_empleado e JOIN ids_duplicados dup 
				 ON e.IdEmpleado = dup.IdEmpleado;
        -- Si la actualización se completa sin errores, confirma la transacción
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Si ocurre un error, se revierte la transacción
        ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE();

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

