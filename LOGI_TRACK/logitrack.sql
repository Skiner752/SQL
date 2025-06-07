create database LogiTrack;
use LogiTrack;

CREATE TABLE Cities (
	City_ID INT PRIMARY KEY,
    City_Name VARCHAR(255)
);
select * from Cities;
drop table Cities;
CREATE TABLE Areas (
	Area_ID INT PRIMARY KEY,
    Area_Name VARCHAR(255)
);
select * from Areas;
drop table Areas;
CREATE TABLE Warehouses (
	Warehouse_ID INT PRIMARY KEY,
    City INT,
    Area INT,
    FOREIGN KEY(City) REFERENCES Cities(City_ID),
    FOREIGN KEY(Area) REFERENCES Areas(Area_ID)
);
select * from Warehouses;
drop table Warehouses;
CREATE TABLE Drivers (
	Driver_ID INT PRIMARY KEY,
    Driver_Name VARCHAR(255),
    Phone_number VARCHAR(255)
);
select * from Drivers;
drop table Drivers;
CREATE TABLE Vehicles (
	Vehicle_ID INT PRIMARY KEY,
    Vehicle_type VARCHAR(255),
    Capacity DECIMAL(10,2)
);
select * from Vehicles;
drop table Vehicles;
CREATE TABLE Routes (
	Route_ID INT PRIMARY KEY,
    Start_point INT,
    End_point INT,
    FOREIGN KEY(Start_point) REFERENCES Warehouses(Warehouse_ID),
    FOREIGN KEY(End_point) REFERENCES Warehouses(Warehouse_ID)
);
select * from Routes;
drop table Routes;
CREATE TABLE Deliveries (
	Delivery_ID INT PRIMARY KEY,
    Order_date DATE,
    Scheduled_delivery_date DATE,
    Route_ID INT,
    Driver_ID INT,
    Vehicle_ID INT,
    Volume DECIMAL(10,2),
    FOREIGN KEY(Route_ID) REFERENCES Routes(Route_ID),
    FOREIGN KEY(Driver_ID) REFERENCES Drivers(Driver_ID),
    FOREIGN KEY(Vehicle_ID) REFERENCES Vehicles(Vehicle_ID)
);
select * from Deliveries;
drop table Deliveries;
CREATE TABLE Delivery_logs (
	ID INT PRIMARY KEY,
    Delivery_id INT,
    Actual_delivery_date DATE NULL,
    Status VARCHAR(255),
    FOREIGN KEY(Delivery_id) REFERENCES Deliveries(Delivery_ID)
);
select * from Delivery_logs;
drop table Delivery_logs;

### 1)Delivery routes with longest average time 
SELECT 
    d.Route_ID,
    ROUND(AVG(DATEDIFF(dl.Actual_delivery_date, d.Order_date)), 2) AS avg_delivery_time_days
FROM Deliveries d
JOIN Delivery_logs dl ON d.Delivery_ID = dl.Delivery_id
WHERE dl.Actual_delivery_date IS NOT NULL
GROUP BY d.Route_ID
ORDER BY avg_delivery_time_days DESC;

### 2)Top 5 most active warehouses 
SELECT 
    w.Warehouse_ID,
    COUNT(*) AS total_activity
FROM Warehouses w
JOIN Routes r ON w.Warehouse_ID = r.Start_point OR w.Warehouse_ID = r.End_point
JOIN Deliveries d ON r.Route_ID = d.Route_ID
GROUP BY w.Warehouse_ID
ORDER BY total_activity DESC
LIMIT 5;


### 3)Drivers with on-time delivery rate over 95% 
SELECT 
    d.Driver_ID,
    dr.Driver_Name,
    COUNT(*) AS total_deliveries,
    SUM(CASE 
            WHEN dl.Actual_delivery_date <= d.Scheduled_delivery_date THEN 1 
            ELSE 0 
        END) AS on_time_deliveries,
    ROUND(SUM(CASE 
                WHEN dl.Actual_delivery_date <= d.Scheduled_delivery_date THEN 1 
                ELSE 0 
              END) / COUNT(*) * 100, 2) AS on_time_rate
FROM Deliveries d
JOIN Delivery_logs dl ON d.Delivery_ID = dl.Delivery_id
JOIN Drivers dr ON d.Driver_ID = dr.Driver_ID
WHERE dl.Actual_delivery_date IS NOT NULL
GROUP BY d.Driver_ID, dr.Driver_Name
HAVING on_time_rate > 95;


### 4)Deliveries delayed by more than 2 days 
SELECT 
    d.Delivery_ID,
    d.Order_date,
    d.Scheduled_delivery_date,
    dl.Actual_delivery_date,
    DATEDIFF(dl.Actual_delivery_date, d.Scheduled_delivery_date) AS delay_days
FROM Deliveries d
JOIN Delivery_logs dl ON d.Delivery_ID = dl.Delivery_id
WHERE 
    dl.Actual_delivery_date IS NOT NULL
    AND DATEDIFF(dl.Actual_delivery_date, d.Scheduled_delivery_date) > 2;

    
### 5)Total volume shipped by vehicle type 
SELECT 
    v.Vehicle_type,
    ROUND(SUM(d.Volume), 2) AS total_volume_shipped
FROM Deliveries d
JOIN Vehicles v ON d.Vehicle_ID = v.Vehicle_ID
GROUP BY v.Vehicle_type
ORDER BY total_volume_shipped DESC;

    
### 6)Daily number of deliveries per city
SELECT 
    d.Order_date,
    c.City_Name,
    COUNT(*) AS delivery_count
FROM Deliveries d
JOIN Routes r ON d.Route_ID = r.Route_ID
JOIN Warehouses w ON r.Start_point = w.Warehouse_ID
JOIN Cities c ON w.City = c.City_ID
GROUP BY d.Order_date, c.City_Name
ORDER BY d.Order_date, c.City_Name;



### Dummy data to test
INSERT INTO Areas(Area_ID, Area_Name) VALUES (500,'Donerka');
START TRANSACTION;
UPDATE Areas
SET Area_Name = 'Dodo pizza'
WHERE Area_ID = 500;
-- COMMIT;
select * from Areas;
ROLLBACK;

INSERT INTO Cities(City_ID, City_Name) VALUES (30,'Temirtay');
START TRANSACTION;
UPDATE Cities
SET City_ID = 31
WHERE City_Name = 'Temirtay';
-- COMMIT;
select * from Cities;
ROLLBACK;

-- Indices
-- 1)
EXPLAIN
SELECT 
    d.Route_ID,
    ROUND(AVG(DATEDIFF(dl.Actual_delivery_date, d.Order_date)), 2) AS avg_delivery_time_days
FROM Deliveries d
JOIN Delivery_logs dl ON d.Delivery_ID = dl.Delivery_id
WHERE dl.Actual_delivery_date IS NOT NULL
GROUP BY d.Route_ID
ORDER BY avg_delivery_time_days DESC;

-- 2)
EXPLAIN
SELECT 
    w.Warehouse_ID,
    COUNT(*) AS total_activity
FROM Warehouses w
JOIN Routes r ON w.Warehouse_ID = r.Start_point OR w.Warehouse_ID = r.End_point
JOIN Deliveries d ON r.Route_ID = d.Route_ID
GROUP BY w.Warehouse_ID
ORDER BY total_activity DESC
LIMIT 5;

-- 4)
EXPLAIN
SELECT 
    d.Delivery_ID,
    d.Order_date,
    d.Scheduled_delivery_date,
    dl.Actual_delivery_date,
    DATEDIFF(dl.Actual_delivery_date, d.Scheduled_delivery_date) AS delay_days
FROM Deliveries d
JOIN Delivery_logs dl ON d.Delivery_ID = dl.Delivery_id
WHERE 
    dl.Actual_delivery_date IS NOT NULL
    AND DATEDIFF(dl.Actual_delivery_date, d.Scheduled_delivery_date) > 2;

 
-- Ускоряет JOIN по Delivery_ID и фильтрацию по Actual_delivery_date IS NOT NULL
CREATE INDEX idx_dl_deliveryid_actualdate
  ON Delivery_logs (Delivery_id, Actual_delivery_date);

-- Ускоряет группировку/AVG по Route_ID и доступ к полю Order_date
CREATE INDEX idx_deliveries_routeid_orderdate
  ON Deliveries (Route_ID, Order_date);



SHOW INDEX FROM Deliveries; 
DROP INDEX idx_logs_date_deliveryid ON Delivery_logs;
DROP INDEX idx_deliveries_route_id ON Deliveries;
DROP INDEX idx_deliverylogs_delivery_id ON Delivery_logs;
DROP INDEX idx_deliveries_route_id ON Deliveries;


