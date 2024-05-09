CREATE DATABASE ElectroFacil;

USE ElectroFacil;

CREATE TABLE ventas (
    IdTransaccion INT,
    Fecha DATE,
    IdCategoria INT,
    IdProducto INT, 
    Producto VARCHAR(255),
    Cantidad INT,
    PrecioUnitario DECIMAL(10, 2),
    TotalVenta DECIMAL(10, 2),
    PRIMARY KEY (IdTransaccion)
);

CREATE DATABASE IF NOT EXISTS ElectroFacil;

USE ElectroFacil;

CREATE TABLE IF NOT EXISTS ventas_consolidadas (
    IdTransaccion INT,
    IdLocal INT,
    Fecha DATE,
    IdCategoria INT,
    IdProducto INT,
    Producto VARCHAR(255),
    Cantidad INT,
    PrecioUnitario DECIMAL(10, 2),
    TotalVenta DECIMAL(10, 2),
    PRIMARY KEY (IdTransaccion, IdLocal)
);

USE ElectroFacil;
SELECT * FROM ventas;

USE ElectroFacil;
SELECT * FROM ventas_consolidadas


