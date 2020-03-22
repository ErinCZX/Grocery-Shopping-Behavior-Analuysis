USE db_consumer_panel;
# a. How many:
#  Store shopping trips are recorded in your database?
SELECT COUNT(TC_id) FROM trips; # 7596145
#  Households appear in your database?
SELECT COUNT(hh_id) FROM households; # 39577
SELECT COUNT(DISTINCT hh_id) FROM trips; # 39577
#  Stores of different retailers appear in our data base?
SELECT COUNT(DISTINCT TC_retailer_code) FROM trips; # 863
#  Different products are recorded?
SELECT COUNT(DISTINCT prod_id) FROM products; # 3153140
# 		  i. Products per category and products per module
DROP TABLE IF EXISTS prodct_by_department;
CREATE TEMPORARY TABLE prodct_by_department
SELECT s.department_at_prod_id, COUNT(s.prod_id) FROM (SELECT DISTINCT prod_id, department_at_prod_id FROM products) AS s GROUP BY s.department_at_prod_id;
DROP TABLE IF EXISTS module_by_department;
CREATE TEMPORARY TABLE module_by_department
SELECT s.department_at_prod_id, COUNT(s.module_at_prod_id) FROM (SELECT DISTINCT module_at_prod_id, department_at_prod_id FROM products) AS s GROUP BY s.department_at_prod_id;
# 		  ii. Plot the distribution of products per module and products per category
SELECT * FROM prodct_by_department INTO OUTFILE 'C://MYSQL_import//a.4.ii.prodct_by_department.csv';
SELECT * FROM module_by_department INTO OUTFILE 'C://MYSQL_import//a.4.ii.module_by_department.csv';

#  Transactions?
#         i. Total transactions and transactions realized under some kind of promotion.
SELECT COUNT(TC_id) FROM purchases;
SELECT COUNT(TC_id) FROM purchases WHERE coupon_value_at_TC_prod_id > 0;

# b. Aggregate the data at the household‐monthly level to answer the following questions:
#  How many households do not shop at least once on a 3 month periods.
DROP TABLE IF EXISTS households_dont_shop_3m;
CREATE TABLE households_dont_shop_3m WITH
max_date_index AS (SELECT 12*(YEAR(MAX(TC_date)) - 2004) + MONTH(MAX(TC_date)) + 1 AS month_index FROM trips),
min_date_index AS (SELECT 12*(YEAR(MIN(TC_date)) - 2004) + MONTH(MIN(TC_date)) - 1 AS month_index FROM trips),
monthly_level_all AS (
	SELECT hh_id, 12*(YEAR(TC_date) - 2004) + MONTH(TC_date) AS month_index FROM trips ORDER BY hh_id, month_index DESC),
monthly_level_unique AS (
	SELECT DISTINCT hh_id, month_index FROM monthly_level_all
    UNION
    SELECT DISTINCT monthly_level_all.hh_id, min_date_index.month_index FROM monthly_level_all, min_date_index
    UNION 
    SELECT DISTINCT monthly_level_all.hh_id, max_date_index.month_index FROM monthly_level_all, max_date_index),
reorder AS ( SELECT * FROM monthly_level_unique GROUP BY hh_id, month_index),
combine AS (
	SELECT C.hh_id AS hh_id1, C.month_index AS month_index1, D.hh_id AS hh_id2, D.month_index AS month_index2
	FROM (SELECT hh_id, month_index, @row1:=@row1+1 AS RN FROM reorder CROSS JOIN (SELECT @row1:=0) a ORDER BY hh_id, month_index) C
	INNER JOIN (SELECT hh_id, month_index, @row2:=@row2+1 AS RN FROM reorder CROSS JOIN (SELECT @row2:=1) b ORDER BY hh_id, month_index) D
	ON C.RN = D.RN)
SELECT hh_id1 AS hh_id, month_index1 - month_index2 AS dont_shop FROM combine WHERE hh_id1 = hh_id2;

SELECT COUNT(1) FROM (SELECT MAX(dont_shop) max FROM households_dont_shop_3m GROUP BY hh_id) a WHERE a.max>2;

DROP TABLE IF EXISTS households_dont_shop_3m_exact;
CREATE TABLE households_dont_shop_3m_exact WITH
max_date_index AS (SELECT DATE(MAX(TC_date) + 1) AS day_index FROM trips),
min_date_index AS (SELECT DATE(MIN(TC_date) - 1) AS day_index FROM trips),
day_level_all AS (
	SELECT hh_id, TC_date AS day_index FROM trips ORDER BY hh_id, day_index DESC),
day_level_unique AS (
	SELECT DISTINCT hh_id, day_index FROM day_level_all
    UNION
    SELECT DISTINCT day_level_all.hh_id, min_date_index.day_index FROM day_level_all, min_date_index
    UNION 
    SELECT DISTINCT day_level_all.hh_id, max_date_index.day_index FROM day_level_all, max_date_index),
reorder AS ( SELECT * FROM day_level_unique GROUP BY hh_id, day_index),
combine AS (
	SELECT C.hh_id AS hh_id1, C.day_index AS day_index1, D.hh_id AS hh_id2, D.day_index AS day_index2
	FROM (SELECT hh_id, day_index, @row1:=@row1+1 AS RN FROM reorder CROSS JOIN (SELECT @row1:=0) a ORDER BY hh_id, day_index) C
	INNER JOIN (SELECT hh_id, day_index, @row2:=@row2+1 AS RN FROM reorder CROSS JOIN (SELECT @row2:=1) b ORDER BY hh_id, day_index) D
	ON C.RN = D.RN)
SELECT hh_id1 AS hh_id, DATEDIFF(day_index1, day_index2) AS dont_shop FROM combine WHERE hh_id1 = hh_id2;

select * from households_dont_shop_3m_exact;
SELECT COUNT(1) FROM (SELECT MAX(dont_shop) max FROM households_dont_shop_3m_exact GROUP BY hh_id) a WHERE a.max > 90;

#         i. Is it reasonable?  
#         ii. Why do you think this is occurring?

#     Loyalism: Among the households who shop at least once a month, which % of them concentrate at least 80% of their 
#     grocery expenditure (on average) on single retailer? And among 2 retailers?
DROP TABLE IF EXISTS retailer_wallet_share_total;
CREATE TABLE retailer_wallet_share_total WITH
percentage_with_no_rank AS (
	SELECT trips.hh_id, trips.TC_retailer_code, SUM(trips.TC_total_spent) / t.total AS percentage
	FROM trips, (SELECT hh_id, SUM(TC_total_spent) total FROM trips GROUP BY hh_id ORDER BY hh_id) AS t
	WHERE trips.hh_id = t.hh_id
	GROUP BY hh_id, TC_retailer_code
	ORDER BY trips.hh_id, percentage DESC),
hh_shop_monthly AS (
	SELECT a.hh_id, SUM(a.flag) shop_times
    FROM (SELECT DISTINCT hh_id, MONTH(TC_date), 1 flag FROM trips) AS a
    GROUP BY a.hh_id
	HAVING shop_times = 12)
SELECT b.*, ROW_NUMBER() OVER (PARTITION BY hh_id ORDER BY percentage DESC) RK 
FROM hh_shop_monthly a LEFT JOIN percentage_with_no_rank b ON a.hh_id = b.hh_id;

# 1 retailer
DROP TABLE IF EXISTS loyalism_id_list_1;
CREATE TABLE loyalism_id_list_1
SELECT hh_id FROM retailer_wallet_share_total
WHERE RK = 1;
ALTER TABLE loyalism_id_list_1 ADD PRIMARY KEY (hh_id);

DROP TABLE IF EXISTS loyalism_list_1;
CREATE TABLE loyalism_list_1
SELECT b.TC_id, b.hh_id, b.TC_date, b.TC_retailer_code, b.TC_total_spent
FROM loyalism_id_list_1 a LEFT JOIN trips b ON a.hh_id = b.hh_id;
ALTER TABLE loyalism_list_1 ADD PRIMARY KEY (TC_id);

WITH
month_spent AS (
	SELECT hh_id, TC_retailer_code, MONTH(TC_date) month, SUM(TC_total_spent) total_spent
	FROM loyalism_list_1
	GROUP BY TC_retailer_code, hh_id, month),
month_spent_max AS (
	SELECT a.hh_id, a.TC_retailer_code, a.total_spent, a.month
    FROM (
		SELECT TC_retailer_code, hh_id, total_spent, month, ROW_NUMBER() OVER (PARTITION BY hh_id, month ORDER BY total_spent DESC) RK
		FROM month_spent
		GROUP BY TC_retailer_code, hh_id, month) AS a
    WHERE a.RK =1),
month_spent_total AS (
	SELECT hh_id, month, SUM(total_spent) total_spent
    FROM month_spent
    GROUP BY hh_id, month),
month_wallet_share AS (
	SELECT a.hh_id, b.TC_retailer_code, a.month, b.total_spent/a.total_spent percentage
    FROM month_spent_total a, month_spent_max b
    WHERE a.hh_id = b.hh_id AND a.month = b.month),
loyalism_at_least_1m AS (
	SELECT hh_id, TC_retailer_code, COUNT(1) count
	FROM month_wallet_share
	WHERE percentage > 0.5 # wallet share can be changed
	GROUP BY hh_id, TC_retailer_code)
#SELECT TC_retailer_code, COUNT(1) count FROM loyalism_at_least_1m WHERE count>=6 GROUP BY TC_retailer_code ORDER BY count DESC;
#SELECT * FROM households WHERE hh_id IN (SELECT hh_id FROM loyalism_at_least_1m WHERE count>=6) 
#INTO OUTFILE 'C://MYSQL_import//loyalism_details.csv';
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=12
UNION ALL
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=11
UNION ALL
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=10
UNION ALL
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=9
UNION ALL
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=8
UNION ALL
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=7
UNION ALL
SELECT COUNT(DISTINCT hh_id) FROM loyalism_at_least_1m WHERE count>=6;

# 2 retailer
DROP TABLE IF EXISTS loyalism_id_list_2;
CREATE TABLE loyalism_id_list_2
SELECT hh_id, SUM(percentage) p 
FROM retailer_wallet_share_total 
WHERE RK IN (1, 2) 
GROUP BY hh_id HAVING p > 0.8;
ALTER TABLE loyalism_id_list_2 ADD PRIMARY KEY (hh_id);

DROP TABLE IF EXISTS loyalism_list_2;
CREATE TABLE loyalism_list_2
SELECT b.TC_id, b.hh_id, b.TC_date, b.TC_retailer_code, b.TC_total_spent
FROM loyalism_id_list_2 a LEFT JOIN trips b ON a.hh_id = b.hh_id;
ALTER TABLE loyalism_list_2 ADD PRIMARY KEY (TC_id);


WITH
loyalism_list_temp AS (
	SELECT hh_id, SUM(percentage) p 
	FROM retailer_wallet_share_total 
	WHERE RK IN (1, 2) 
	GROUP BY hh_id HAVING p > 0.8),
loyalism_benchmark AS (
	SELECT a.hh_id, b.TC_retailer_code, b.percentage - 0.8 benchmark
    FROM loyalism_list_temp a LEFT JOIN retailer_wallet_share_total b ON a.hh_id = b.hh_id
    WHERE b.RK = 1
    HAVING benchmark > 0)
SELECT * FROM loyalism_benchmark;
select * from retailer_wallet_share_total;

#         i. Are their demographics remarkably different? Are these people richer? Poorer?
#         ii. What is the retailer that has more loyalists?
#         iii. Where do they live? Plot the distribution by state.

DROP TABLE IF EXISTS loyalism_one_retailer;
CREATE TEMPORARY TABLE loyalism_one_retailer WITH 
loyalist AS (SELECT DISTINCT hh_id FROM retailer_wallet_share WHERE percentage > 0.8)
SELECT b.hh_state, COUNT(a.hh_id) count FROM loyalist a LEFT JOIN households b ON a.hh_id = b.hh_id GROUP BY b.hh_state;
SELECT * FROM loyalism_one_retailer INTO OUTFILE 'C://MYSQL_import//b.2.iii.loyalism_one_retailer.csv';

DROP TABLE IF EXISTS loyalism_two_retailer;
CREATE TEMPORARY TABLE loyalism_two_retailer WITH 
loyalist AS (SELECT a.hh_id FROM (SELECT hh_id, SUM(percentage) AS percentage FROM retailer_wallet_share WHERE RK IN (1, 2) GROUP BY hh_id) AS a WHERE a.percentage > 0.8)
SELECT b.hh_state, COUNT(a.hh_id) count FROM loyalist a LEFT JOIN households b ON a.hh_id = b.hh_id GROUP BY b.hh_state;
SELECT * FROM loyalism_two_retailer INTO OUTFILE 'C://MYSQL_import//b.2.iii.loyalism_two_retailer.csv';

#  Plot with the distribution:
#         i. Average number of items purchased on a given month.
ALTER TABLE purchases ADD INDEX index_tc_id_p(TC_id);
CREATE TABLE avg_num_item 
SELECT MONTH(TC_date) month, SUM(b.quantity_at_TC_prod_id)/COUNT(DISTINCT b.prod_id)
FROM trips a LEFT JOIN purchases b ON a.TC_id = b.TC_id GROUP BY month;
SELECT * FROM avg_num_item INTO OUTFILE 'C://MYSQL_import//b.3.i.avg_num_item_purchased.csv';

#         ii. Average number of shopping trips per month.
# we assume that it is average shopping trips per family
DROP TABLE IF EXISTS avg_num_shopping;
CREATE TABLE avg_num_shopping  
SELECT month, COUNT(1)/COUNT(DISTINCT hh_id) AS number_of_shopping FROM (SELECT TC_id, hh_id, MONTH(TC_date) AS month FROM trips) AS a GROUP BY month;
SELECT * FROM avg_num_shopping; INTO OUTFILE 'C://MYSQL_import//b.3.ii.avg_num_shopping.csv';

#         iii. Average number of days between 2 consecutive shopping trips.
SELECT hh_id, AVG(dont_shop) FROM households_dont_shop_3m_exact GROUP BY hh_id 
INTO OUTFILE 'C://MYSQL_import//b.3.iii.avg_days_between_shopping.csv';

# c. Answer and reason the following questions: (Make informative visualizations)
#  Is the number of shopping trips per month correlated with the average number of items purchased?

#  Is the average price paid per item correlated with the number of items purchased?
DROP TABLE IF EXISTS avg_price_item_paid;
CREATE TABLE avg_price_item_paid
SELECT prod_id, AVG(quantity_at_TC_prod_id * total_price_paid_at_TC_prod_id - coupon_value_at_TC_prod_id) AS avg_paid FROM purchases GROUP BY prod_id;
ALTER TABLE avg_price_item_paid ADD PRIMARY KEY (prod_id);
SELECT * FROM avg_price_item_paid INTO OUTFILE 'C://MYSQL_import//c.2.avg_price_item_paid.csv';


DROP TABLE IF EXISTS avg_num_purchased_by_item;
CREATE TABLE avg_num_purchased_by_item
SELECT prod_id, COUNT(1) AS item_purchased FROM purchases GROUP BY prod_id;
ALTER TABLE avg_num_purchased_by_item ADD PRIMARY KEY (prod_id);
SELECT * FROM avg_num_purchased_by_item INTO OUTFILE 'C://MYSQL_import//c.2.avg_num_purchased_by_item.csv';

SELECT a.prod_id, a.avg_paid, b.item_purchased
FROM avg_price_item_paid a LEFT JOIN avg_num_purchased_by_item b ON a.prod_id = b.prod_id
INTO OUTFILE 'C://MYSQL_import//c.2.corr_avgprice_num.csv';

#  Private Labeled products are the products with the same brand as the supermarket. In the data set they appear labeled as ‘CTL BR’
#         i. What are the product categories that have proven to be more “Private labelled”
SELECT department_at_prod_id, COUNT(1) count FROM products WHERE brand_at_prod_id REGEXP 'CTL BR' GROUP BY department_at_prod_id ORDER BY count DESC;

#         ii. Is the expenditure share in Private Labeled products constant across months?
DROP TABLE IF EXISTS expenditure_share;
CREATE TABLE expenditure_share WITH 
exp_table AS (SELECT TC_id, prod_id, quantity_at_TC_prod_id*(total_price_paid_at_TC_prod_id-coupon_value_at_TC_prod_id) AS exp FROM purchases ORDER BY TC_id),
month_table AS (SELECT TC_id, (YEAR(TC_date) - 2004)*12 + MONTH(TC_date) AS month FROM trips ORDER BY TC_id),
prod_month_table AS (
	SELECT a.prod_id, b.month, SUM(a.exp) exp
	FROM exp_table a LEFT JOIN month_table b ON a.TC_id = b.TC_id
	GROUP BY a.prod_id, b.month
	ORDER BY a.prod_id),
prod_name_table AS (
	SELECT a.month, CASE WHEN b.brand_at_prod_id REGEXP '^CTL' THEN 1 ELSE 0 END AS is_pl, a.exp
    FROM prod_month_table a LEFT JOIN products b ON a.prod_id = b.prod_id)
SELECT month, SUM(is_pl*exp)/SUM(exp) FROM prod_name_table GROUP BY month;

SELECT * FROM expenditure_share INTO OUTFILE 'C://MYSQL_import//c.3.ii.expenditure_share_monthly.csv';

#         iii. Cluster households in three income groups, Low, Medium and High. Report the average monthly expenditure on 
#         grocery. Study the % of private label share in their monthly expenditures. Use visuals to represent the intuition you are suggesting.
DROP TABLE IF EXISTS expenditure_share_by_income;
CREATE TABLE expenditure_share_by_income WITH
tc_class AS (
	SELECT a.TC_id, 
		   (YEAR(a.TC_date) - 2004)*12 + MONTH(a.TC_date) AS month,
           CASE WHEN b.hh_income<10 THEN 1 WHEN b.hh_income<19 THEN 2 ELSE 3 END AS class
	FROM trips a LEFT JOIN households b ON a.hh_id = b.hh_id ORDER BY a.TC_id),
prod_name_table AS (
	SELECT prod_id, CASE WHEN brand_at_prod_id REGEXP '^CTL' THEN 1 ELSE 0 END AS is_pl
    FROM products)
SELECT b.class,
	   b.month,
       c.is_pl,
	   a.quantity_at_TC_prod_id*(a.total_price_paid_at_TC_prod_id-a.coupon_value_at_TC_prod_id) AS exp
FROM purchases a LEFT JOIN tc_class b ON a.TC_id = b.TC_id LEFT JOIN prod_name_table c ON a.prod_id = c.prod_id;

SELECT month, class, SUM(is_pl*exp)/SUM(exp) AS share
FROM expenditure_share_by_income
GROUP BY class, month
INTO OUTFILE 'C://MYSQL_import//c.3.ii.expenditure_share_monthly_by_income.csv';
