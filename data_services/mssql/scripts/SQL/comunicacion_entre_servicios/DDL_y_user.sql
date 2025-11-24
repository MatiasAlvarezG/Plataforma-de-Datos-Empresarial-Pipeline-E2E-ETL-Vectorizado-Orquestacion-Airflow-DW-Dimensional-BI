USE comunication_db;
GO 

CREATE TABLE mensajes (
    id BIGINT IDENTITY PRIMARY KEY,
    CID VARCHAR(36) NOT NULL,                       -- UUID para correlacionar solicitud-respuesta
    mensaje VARCHAR(50) NOT NULL,                   -- "crear_db", "iniciar_modelo", "cerrar_listener", "procesar_area_procesamiento", "procesar_y_iniciar_modelo"
    emisor VARCHAR(50) NOT NULL,
    receptor VARCHAR(50) NOT NULL,
    fecha_creacion DATETIME2 DEFAULT SYSDATETIME(),
    estado VARCHAR(20) NOT NULL,                    -- "pendiente", "procesado"
    resultado NVARCHAR(16)                          -- "OK", "FAIL", "ALREADY DONE"
);


-- Se crea inicio de sesión para la instancia de SQL Server
CREATE LOGIN com_user WITH PASSWORD = 'C0m_p4ssw0rd.';

-- Se crea el usuario com_user
CREATE USER com_user FOR LOGIN com_user;

-- Se da privilegios de lectura y inserción
GRANT SELECT, INSERT ON dbo.mensajes TO com_user;
