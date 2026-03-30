--Join tables
SELECT DISTINCT d.name AS 'merchant name', e.name AS 'category', c.date AS 'transaction date', c.card AS 'card number', ROUND(c.amount,2) AS 'transaction amount'
FROM transaction AS c
JOIN merchant AS d
ON c.id_merchant = d.id
JOIN merchant_category AS e
ON d.id_merchant_category = e.id

--Create table fraud_summary
CREATE TABLE [dbo].[fraud_summary](
	[merchant_name] [varchar](50) NOT NULL,
	[category] [varchar](max) NOT NULL,
	[transaction_date] [varchar](max) NOT NULL,
	[card_number] [bigint] NOT NULL,
	[transaction_amount] [money] NOT NULL
)

INSERT INTO [fraud_summary]

SELECT DISTINCT d.name AS 'merchant name', e.name AS 'category', c.date AS 'transaction date', c.card AS 'card number', ROUND(c.amount,2) AS 'transaction amount'
FROM [transaction] AS c
JOIN merchant AS d
ON c.id_merchant = d.id
JOIN merchant_category AS e
ON d.id_merchant_category = e.id

--Detect transaction value
SELECT DISTINCT merchant_name, AVG(transaction_amount), transaction_date,
CASE 
WHEN AVG(transaction_amount) > 1000 THEN 'High Value Order'
WHEN AVG(transaction_amount) BETWEEN 500 AND 999 THEN 'Medium Order Value'
ELSE 'Low Order Value'
END AS 'order_status'
FROM fraud_summary
GROUP BY merchant_name, transaction_date

--Detect after hour transactions
SELECT merchant_name, transaction_date
FROM fraud_summary
WHERE DATEPART(HOUR, transaction_date) NOT BETWEEN 6 AND 22;

--Detect multiple transactions from one card
SELECT card_number, COUNT(DISTINCT merchant_name) AS unique_merchants, COUNT(*) AS total_transactions
FROM fraud_summary
GROUP BY card_number
HAVING COUNT(DISTINCT merchant_name) > 1
AND COUNT(*) > 1