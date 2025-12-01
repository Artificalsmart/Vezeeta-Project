create database vezeeta
use vezeeta

-- total number of orders 
select count(distinct id) as total_orders from Orders 

create view totalorders as select count(distinct id) as total_orders from Orders 


--List the Order Sources ordered by number of orders.
select o.Source , count(o.id) as number_of_orders from Orders o
group by o.Source
order by number_of_orders 

create view source_orders as 
select o.Source , count(o.id) as number_of_orders from Orders o
group by o.Source


--Calculate the overall fulfilment rate (Delivered Orders / Total Orders)

	SELECT 
		round(CAST(SUM(CASE WHEN ost.StateName = 'Delivered' THEN 1 ELSE 0 END) AS FLOAT) 
		/ COUNT(*)*100  ,2) as fulfilment_rate
	FROM Orders o inner join [dbo].[OrderStateTypes] ost
	on o.OrderStateTypeId =ost.OrderStateTypeId

	create view overall_fulfilment as 
	SELECT 
		round(CAST(SUM(CASE WHEN ost.StateName = 'Delivered' THEN 1 ELSE 0 END) AS FLOAT) 
		/ COUNT(*)*100  ,2) as fulfilment_rate
	FROM Orders o inner join [dbo].[OrderStateTypes] ost
	on o.OrderStateTypeId =ost.OrderStateTypeId

--Find the average order value month over month 
with averageordervalue as(
SELECT 
    YEAR(o.CreatedON) AS OrderYear,
    MONTH(o.CreatedON) AS OrderMonth,
    COUNT(DISTINCT o.id) AS TotalOrders,
    SUM(oi.price * oi.quantity) AS TotalRevenue,
    ROUND(SUM(oi.price * oi.quantity) * 1.0 / COUNT(DISTINCT o.id), 2) AS aov
FROM Orders o
INNER JOIN [dbo].[OrderItems] oi
    ON o.Id = oi.orderid
GROUP BY YEAR(o.CreatedON) ,MONTH(o.CreatedON)
)
select OrderYear,OrderMonth,aov ,
lag(aov) over(order by OrderYear , OrderMonth ) as prevAOV,
cast((aov - lag(aov) over(order by orderyear ,ordermonth)) * 100.0/nullif(lag(aov) over(order by orderyear,ordermonth),0) as decimal(10,2)) as MOM_AOV_percentage
from averageordervalue
ORDER BY orderyear , ordermonth

create view aov_mom as

with averageordervalue as(
SELECT 
    YEAR(o.CreatedON) AS OrderYear,
    MONTH(o.CreatedON) AS OrderMonth,
    COUNT(DISTINCT o.id) AS TotalOrders,
    SUM(oi.price * oi.quantity) AS TotalRevenue,
    ROUND(SUM(oi.price * oi.quantity) * 1.0 / COUNT(DISTINCT o.id), 2) AS aov
FROM Orders o
INNER JOIN [dbo].[OrderItems] oi
    ON o.Id = oi.orderid
GROUP BY YEAR(o.CreatedON) ,MONTH(o.CreatedON)
)
select OrderYear,OrderMonth,aov ,
lag(aov) over(order by OrderYear , OrderMonth ) as prevAOV,
cast((aov - lag(aov) over(order by orderyear ,ordermonth)) * 100.0/nullif(lag(aov) over(order by orderyear,ordermonth),0) as decimal(10,2)) as MOM_AOV_percentage
from averageordervalue



--Retrieve the number of scheduled orders and their fulfilment rate we have two types of scheduled (scheduled and scheuled vezeeta)
SELECT
   count(case when statename = 'Scheduled' then 1 END) AS ScheduledOrders,
    COUNT(CASE WHEN statename = 'Delivered' THEN 1 END) AS DeliverdOrders,
    COUNT(CASE WHEN statename = 'Scheduled' THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN statename = 'Delivered' THEN 1 END),0) AS FulfilmentRate
FROM Orders o  full join [dbo].[OrderStateTypes] ost 
on o.OrderStateTypeId=ost.OrderStateTypeId



Create view scheduledorders_fulfilmentrate as 
SELECT
	count(case when statename = 'Scheduled' then 1 END) AS ScheduledOrders,
    COUNT(CASE WHEN statename = 'Delivered' THEN 1 END) AS DeliverdOrders,
    COUNT(CASE WHEN statename = 'Scheduled' THEN 1 END) * 1.0
        / NULLIF(COUNT(CASE WHEN statename = 'Delivered' THEN 1 END),0) AS FulfilmentRate
FROM Orders o  full join [dbo].[OrderStateTypes] ost 
on o.OrderStateTypeId=ost.OrderStateTypeId

--Find the top 3 reasons for not fulfilled orders with their percentages from total orders.  
--for no fulfilled (Dispensed,Canceled,Rejected By Pharmacy,Rejected By Delivery,Rejected By Vezeeta,Pay. Failed,Cancelled,Items Not Updated)

SELECT TOP 3  ost.StateName AS NotFulfilmentReason,
    COUNT(DISTINCT o.Id) AS NumberOfOrders,
    ROUND(COUNT(DISTINCT o.Id) * 100.0 / (SELECT COUNT(*) FROM Orders), 2) AS PercentageOfTotal
FROM Orders o
INNER JOIN OrderStateTypes ost ON o.OrderStateTypeId = ost.OrderStateTypeId
WHERE ost.StateName IN (
    'Dispensed',
	'Canceled',
    'Rejected By Pharmacy',
    'Rejected By Delivery',
    'Rejected By Vezeeta',
    'Pay. Failed',
    'Items Not Updated',
	'Cancelled'
)
GROUP BY ost.StateName
ORDER BY NumberOfOrders DESC;


create view notfulfilment_top3 as
SELECT TOP 3  ost.StateName AS NotFulfilmentReason,
    COUNT(DISTINCT o.Id) AS NumberOfOrders,
    ROUND(COUNT(DISTINCT o.Id) * 100.0 / (SELECT COUNT(*) FROM Orders), 2) AS PercentageOfTotal
FROM Orders o
INNER JOIN OrderStateTypes ost ON o.OrderStateTypeId = ost.OrderStateTypeId
WHERE ost.StateName IN (
    'Dispensed',
	'Canceled',
    'Rejected By Pharmacy',
    'Rejected By Delivery',
    'Rejected By Vezeeta',
    'Pay. Failed',
    'Items Not Updated',
	'Cancelled'
)
GROUP BY ost.StateName
ORDER BY NumberOfOrders DESC;

-----------------------------------
--Product Analysis
-----------------------------------



--Product analysis based on data doesn't have a product name we can got a product key in orderitems 

--first we need to make non clustered index on product key to make it run fast 
CREATE NONCLUSTERED INDEX IX_IndexName
ON [dbo].[OrderItems] (ProductKey);

--apply the sql statement
--Identify the top 5 Products with the highest number of orders.
select top 5 oi.ProductKey , count(distinct o.id) as total_of_orders from orders o inner join [dbo].[OrderItems] oi
on o.id =oi.OrderId
group by oi.productkey
order by total_of_orders desc

create view top_highest_product_orders as
select top 5 oi.ProductKey , count(distinct o.id) as total_of_orders from orders o inner join [dbo].[OrderItems] oi
on o.id =oi.OrderId
group by oi.productkey
order by total_of_orders desc


--Identify the top 5 Products with the highest amount of Sales Value.

select top 5 oi.ProductKey ,sum(oi.Quantity*oi.Price) as amount from orderitems oi
group by oi.ProductKey
order by amount desc

create view top_highest_product_revenue as
select top 5 oi.ProductKey ,sum(oi.Quantity*oi.Price) as amount from orderitems oi
group by oi.ProductKey
order by amount desc

---------------------------------------------------------------------------
--Pharmacies Analysis
---------------------------------------------------------------------------

create nonclustered index pharme_idx
on [dbo].[Pharmacies] ([o_Key])


--Identify the top 5 Pharmacies based on the number of orders.
select top 5 p.o_Key , count(distinct o.id) as number_of_orders from Pharmacies p inner join Orders o
on o.PharmacyKey =p.o_Key
group by p.o_Key
order by number_of_orders desc

create view highest_pharmacies_orders as
select top 5 p.o_Key , count(distinct o.id) as number_of_orders from Pharmacies p inner join Orders o
on o.PharmacyKey =p.o_Key
group by p.o_Key
order by number_of_orders desc

--Identify the top 5 Pharmacies based on fulfilment rate
select top 5 p.o_Key ,
round(cast(sum(case when os.StateName='Delivered' then 1 else 0 end)*100 as float)/count(distinct o.id), 2)as fulfilment_rate
from Pharmacies p inner join orders o 
on p.o_Key=o.PharmacyKey inner join OrderStateTypes os
on o.OrderStateTypeId =os.OrderStateTypeId
group by p.o_Key
order by fulfilment_rate desc

create view top_pharmacies_fulfilment_rate as
select top 5 p.o_Key ,
round(cast(sum(case when os.StateName='Delivered' then 1 else 0 end) as float)/count(distinct o.id), 2)as fulfilment_rate
from Pharmacies p inner join orders o 
on p.o_Key=o.PharmacyKey inner join OrderStateTypes os
on o.OrderStateTypeId =os.OrderStateTypeId
group by p.o_Key
order by fulfilment_rate desc

--• For each pharmacy, calculate the total value of sales. 
select p.o_Key  , sum(oi.Price*oi.Quantity) as total_revenue from Pharmacies p inner join orders o 
on p.o_Key =o.PharmacyKey inner join OrderItems oi
on o.Id=oi.OrderId
group by p.o_Key

create view pharmacies_total_sales as
select p.o_Key  , sum(oi.Price*oi.Quantity) as total_revenue from Pharmacies p inner join orders o 
on p.o_Key =o.PharmacyKey inner join OrderItems oi
on o.Id=oi.OrderId
group by p.o_Key


--List all the pharmacies with their customer time (Time between order creation and the last order state) in ascending order 
-- i used to calculate it based on pharmacy key and user key and but two conditions
--as success in test flag and also as delivered to make it simple and check manualy 
--if the calculation of time true and get the target or not

select p.o_Key,o.UserKey,o.Id,
CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, max(o.CreatedOn), max(os.TimeStamp)), 0), 108) AS TimeDiffsecond
from Pharmacies p inner join Orders o
on p.o_Key=o.PharmacyKey inner join OrderStates os
on o.id=os.OrderId 
where p.[Test Flag]=1 and os.OrderStateTypeId=4
group by p.o_Key,o.UserKey,o.id
order by TimeDiffsecond

create view pharmacies_diff_customer_t as
select p.o_Key,o.UserKey,o.Id,
CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, max(o.CreatedOn), max(os.TimeStamp)), 0), 108) AS TimeDiffsecond
from Pharmacies p inner join Orders o
on p.o_Key=o.PharmacyKey inner join OrderStates os
on o.id=os.OrderId 
where p.[Test Flag]=1 and os.OrderStateTypeId=4
group by p.o_Key,o.UserKey,o.id


--List all the pharmacies with their customer time (Time between order creation and the last order state) in ascending order 
--the full calculation that's on all situation 
select p.o_Key,
CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, max(o.CreatedOn), max(os.TimeStamp)), 0), 108) AS TimeDiffsecond
from Pharmacies p inner join Orders o
on p.o_Key=o.PharmacyKey inner join OrderStates os
on o.id=os.OrderId 
group by p.o_Key
order by TimeDiffsecond desc


create view pharmacies_diff_customer_t_allsituations as 
select p.o_Key,o.UserKey,o.Id,
CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, max(o.CreatedOn), max(os.TimeStamp)), 0), 108) AS TimeDiffsecond
from Pharmacies p inner join Orders o
on p.o_Key=o.PharmacyKey inner join OrderStates os
on o.id=os.OrderId 
group by p.o_Key,o.UserKey,o.id




--List all the pharmacies with their delivery time (Time between accepting the order and order delivery) in ascending order 
SELECT 
    p.o_Key,
    o.id AS Orderid,
    convert(varchar,dateadd(MINUTE ,DATEDIFF(minute, 
        MAX(CASE WHEN os.OrderStateTypeId = 7 THEN os.timestamp END), 
        MAX(CASE WHEN os.OrderStateTypeId = 4 THEN os.timestamp END)
    ),0),108) AS DateDiffInMinutes
FROM Pharmacies p
INNER JOIN Orders o 
    ON p.o_Key = o.PharmacyKey
INNER JOIN OrderStates os 
    ON o.id = os.OrderId
GROUP BY p.o_Key, o.id
ORDER BY DateDiffInMinutes asc;

create view between_accepting_delivered as
SELECT 
    p.o_Key,
    o.id AS Orderid,
    convert(varchar,dateadd(MINUTE ,DATEDIFF(minute, 
        MAX(CASE WHEN os.OrderStateTypeId = 7 THEN os.timestamp END), 
        MAX(CASE WHEN os.OrderStateTypeId = 4 THEN os.timestamp END)
    ),0),108) AS DateDiffInMinutes
FROM Pharmacies p
INNER JOIN Orders o 
    ON p.o_Key = o.PharmacyKey
INNER JOIN OrderStates os 
    ON o.id = os.OrderId
GROUP BY p.o_Key, o.id




--List the top 5 order states before rejecting the orders (Only for rejected orders) with their total number of orders.
--ranked orders that will be based on max time stamp  
WITH RankedStates AS (
    SELECT 
        o.id AS OrderID,
        ost.StateName,
        os.Timestamp,
        ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY os.Timestamp DESC) AS row_num
    FROM Orders o
    INNER JOIN OrderStates os 
        ON o.id = os.OrderId
    INNER JOIN OrderStateTypes ost 
        ON os.OrderStateTypeId = ost.OrderStateTypeId
    WHERE ost.StateName IN ('Rejected By Pharmacy','Rejected By Delivery','Rejected By Vezeeta')
),
StateBeforeReject AS (
    SELECT 
        o.id AS OrderID,
        LAG(ost.StateName) OVER (PARTITION BY o.id ORDER BY os.Timestamp) AS StateBeforeReject
    FROM Orders o
    INNER JOIN OrderStates os 
        ON o.id = os.OrderId
    INNER JOIN OrderStateTypes ost 
        ON os.OrderStateTypeId = ost.OrderStateTypeId
)
SELECT TOP 5 
    s.StateBeforeReject,
    COUNT(DISTINCT s.OrderID) AS NumberOfOrders
FROM StateBeforeReject s
WHERE s.StateBeforeReject NOT IN ('Rejected By Pharmacy','Rejected By Delivery','Rejected By Vezeeta')
GROUP BY s.StateBeforeReject
ORDER BY NumberOfOrders DESC;


create view  top5_situations_before_rejecting as
WITH RankedStates AS (
    SELECT 
        o.id AS OrderID,
        ost.StateName,
        os.Timestamp,
        ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY os.Timestamp DESC) AS row_num
    FROM Orders o
    INNER JOIN OrderStates os 
        ON o.id = os.OrderId
    INNER JOIN OrderStateTypes ost 
        ON os.OrderStateTypeId = ost.OrderStateTypeId
    WHERE ost.StateName IN ('Rejected By Pharmacy','Rejected By Delivery','Rejected By Vezeeta')
),
StateBeforeReject AS (
    SELECT 
        o.id AS OrderID,
        LAG(ost.StateName) OVER (PARTITION BY o.id ORDER BY os.Timestamp) AS StateBeforeReject
    FROM Orders o
    INNER JOIN OrderStates os 
        ON o.id = os.OrderId
    INNER JOIN OrderStateTypes ost 
        ON os.OrderStateTypeId = ost.OrderStateTypeId
)
SELECT TOP 5 
    s.StateBeforeReject,
    COUNT(DISTINCT s.OrderID) AS NumberOfOrders
FROM StateBeforeReject s
WHERE s.StateBeforeReject NOT IN ('Rejected By Pharmacy','Rejected By Delivery','Rejected By Vezeeta')
GROUP BY s.StateBeforeReject
ORDER BY NumberOfOrders DESC;

