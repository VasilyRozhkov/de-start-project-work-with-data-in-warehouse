-- таблица для хранения дат загрузки
DROP TABLE IF EXISTS dwh.load_dates_customer_report;
create table dwh.load_dates_customer_report (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    constraint load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

-- основная витрина данных
DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id BIGINT NOT NULL,
    customer_name VARCHAR NOT NULL,
    customer_address VARCHAR NOT NULL,
    customer_birthday DATE NOT NULL,
    customer_email VARCHAR NOT null,
    customer_money NUMERIC(15, 2) DEFAULT 0,
    platform_money NUMERIC(15, 2) DEFAULT 0,
    count_orders INTEGER DEFAULT 0, -- !
    avg_price_order NUMERIC(10,2) DEFAULT 0,   
    median_time_order_completed NUMERIC(5,1),
    top_product_category VARCHAR NOT NULL, 
    popular_craftsman_id BIGINT NOT NULL,
    count_order_created BIGINT NOT NULL DEFAULT 0,
    count_order_in_progress BIGINT NOT NULL DEFAULT 0,
    count_order_delivery BIGINT NOT NULL DEFAULT 0,
    count_order_done BIGINT NOT NULL DEFAULT 0,
    count_order_not_done BIGINT NOT NULL DEFAULT 0,
    report_period VARCHAR(7) NOT NULL,
    -- из формулировки задания не понял нужно ли дополнтельно вытащить год и месяц. сделал и закомментил.
--    report_year INTEGER NOT NULL,
--    report_month INTEGER NOT NULL,
    load_dttm TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    constraint customer_report_datamart_pk PRIMARY KEY (id),
    constraint fk_customer FOREIGN KEY (customer_id) REFERENCES dwh.d_customer(customer_id),
    constraint fk_craftsman FOREIGN KEY (popular_craftsman_id) REFERENCES dwh.d_craftsman(craftsman_id)
);

-- индексы для оптимизации запросов
CREATE INDEX idx_customer_report_customer_id ON dwh.customer_report_datamart(customer_id);
CREATE INDEX idx_customer_report_load_dttm ON dwh.customer_report_datamart(load_dttm);

with
dwh_delta as (
    select
    	dcs.customer_id,
    	dcs.customer_name,
    	dcs.customer_address,
    	dcs.customer_birthday,
    	dcs.customer_email,
 		dc.craftsman_id,
        fo.order_id,
        dp.product_id,
        dp.product_price,
        dp.product_type,
        DATE_PART('year', AGE(dcs.customer_birthday)) AS customer_age,
        (fo.order_completion_date - fo.order_created_date) AS diff_order_date, 
        fo.order_status AS order_status,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        crd.customer_id AS exist_customer_id,
        dc.load_dttm AS craftsman_load_dttm,
        dcs.load_dttm AS customers_load_dttm,
        dp.load_dttm AS products_load_dttm
	from dwh.f_order fo 
    join dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
    join dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
    join dwh.d_product dp ON fo.product_id = dp.product_id 
    left join dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id
    where 
    	(fo.load_dttm > (select COALESCE(MAX(load_dttm),'1900-01-01') from dwh.load_dates_craftsman_report_datamart)) or
        (dc.load_dttm > (select COALESCE(MAX(load_dttm),'1900-01-01') from dwh.load_dates_craftsman_report_datamart)) or
        (dcs.load_dttm > (select COALESCE(MAX(load_dttm),'1900-01-01') from dwh.load_dates_craftsman_report_datamart)) or
        (dp.load_dttm > (select COALESCE(MAX(load_dttm),'1900-01-01') from dwh.load_dates_craftsman_report_datamart))
),
dwh_update_delta as ( 
    select     
    	dd.exist_customer_id as customer_id
    from dwh_delta dd 
    where dd.exist_customer_id is not null     
),
dwh_delta_insert_result_pred as ( 
    select  
    	T4.customer_id,
        T4.customer_name,
        T4.customer_address,
        T4.customer_birthday,
        T4.customer_email,
        T4.customer_money,
        T4.platform_money,
        T4.count_orders,
        T4.avg_price_order,
        T4.product_type AS top_product_category,
        T4.median_time_order_completed,
        T4.count_order_created,
        T4.count_order_in_progress,
        T4.count_order_delivery,
        T4.count_order_done,
        T4.count_order_not_done,
        T4.report_period
	from (
    	select     
        	*,
        	RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product 
      	from ( 
        	select 
            	T1.customer_id,
            	T1.customer_name,
             	T1.customer_address,
            	T1.customer_birthday,
              	T1.customer_email,
              	SUM(T1.product_price) * 0.9 AS customer_money,
               	SUM(T1.product_price) * 0.1 AS platform_money,
           		COUNT(order_id) AS count_orders,
              	AVG(T1.product_price) AS avg_price_order,
               	PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
               	SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
             	SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
              	SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
               	SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
              	SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
              	T1.report_period AS report_period
	  		from dwh_delta as T1
	        where T1.exist_customer_id is null
	        group by 
	         	T1.customer_id, 
	            T1.customer_name, 
	            T1.customer_address, 
	            T1.customer_birthday, 
	            T1.customer_email, 
	            T1.report_period
		) AS T2 
         join (
          	select
            	dd.customer_id AS customer_id_for_product_type, 
             	dd.product_type, 
              	COUNT(dd.product_id) AS count_product
             from dwh_delta AS dd
             group by 
            	dd.customer_id, 
               	dd.product_type
             order by count_product desc
       	) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
	) as T4 
	where T4.rank_count_product = 1 
    order by report_period
),
dwh_delta_insert_result as (
	select *
	from (
		select 
			t1.*,
			t2.craftsman_id as popular_craftsman_id,
			RANK() OVER(PARTITION BY customer_id ORDER BY count_product DESC) AS rank_count_craftsman
		from dwh_delta_insert_result_pred as t1
	    join (
	    	select
	        	dd.customer_id AS customer_id_for_product_type, 
	        	dd.craftsman_id,
	        	COUNT(dd.product_id) AS count_product
	        from dwh_delta AS dd
	        group by 
	        	dd.customer_id, 
	            dd.craftsman_id
	        order by count_product desc
	   ) AS t2 ON t1.customer_id = t2.customer_id_for_product_type
   ) as t
   where rank_count_craftsman = 1
),
dwh_delta_update_result_pred as (
   	select 
    	T4.customer_id,
        T4.customer_name,
        T4.customer_address,
        T4.customer_birthday,
        T4.customer_email,
        T4.customer_money,
        T4.platform_money,
        T4.count_orders,
        T4.avg_price_order,
        T4.product_type as top_product_category,
        T4.median_time_order_completed,
        T4.count_order_created,
        T4.count_order_in_progress,
        T4.count_order_delivery, 
        T4.count_order_done, 
        T4.count_order_not_done,
         T4.report_period 
	from (
    	select
         	*,
         	RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product
        from (
         	select 
            	T1.customer_id,
               	T1.customer_name,
                T1.customer_address,
                T1.customer_birthday,
                T1.customer_email,
                SUM(T1.product_price) - (SUM(T1.product_price) * 0.1) AS customer_money,
                SUM(T1.product_price) * 0.1 AS platform_money,
                COUNT(order_id) AS count_orders,
                AVG(T1.product_price) AS avg_price_order,
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, 
                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
               	SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                T1.report_period AS report_period
           	from (
            	select
                	dcs.customer_id,
                    dcs.customer_name,
                    dcs.customer_address,
                    dcs.customer_birthday,
                    dcs.customer_email,
                    dc.craftsman_id,
                    fo.order_id AS order_id,
                    dp.product_id,
                    dp.product_price,
                    dp.product_type,
                    DATE_PART('year', AGE(dcs.customer_birthday)) AS customer_age,
                    fo.order_completion_date - fo.order_created_date AS diff_order_date,
                    fo.order_status AS order_status, 
                    TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
              	from dwh.f_order fo 
              	join dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                join dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                join dwh.d_product dp ON fo.product_id = dp.product_id
                join dwh_update_delta ud ON fo.customer_id = ud.customer_id
          	) AS T1
            group by 
            	T1.customer_id, 
                T1.customer_name, 
                T1.customer_address, 
                T1.customer_birthday, 
                T1.customer_email, 
                T1.report_period
		) AS T2 
        join (
        	select
            	dd.customer_id AS customer_id_for_product_type, 
                dd.product_type, 
                COUNT(dd.product_id) AS count_product
            from dwh_delta AS dd
            group by 
            	dd.customer_id, 
            	dd.product_type
           order by count_product desc
       	) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
	) AS T4 
	where T4.rank_count_product = 1 ORDER BY report_period
),
dwh_delta_update_result as (
	select *
	from (
		select 
			t1.*,
			t2.craftsman_id as popular_craftsman_id,
			RANK() OVER(PARTITION BY customer_id ORDER BY count_product DESC) AS rank_count_craftsman
		from dwh_delta_update_result_pred as t1
	    join (
	    	select
	        	dd.customer_id AS customer_id_for_product_type, 
	        	dd.craftsman_id,
	        	COUNT(dd.product_id) AS count_product
	        from dwh_delta AS dd
	        group by 
	        	dd.customer_id, 
	            dd.craftsman_id
	        order by count_product desc
	   ) AS t2 ON t1.customer_id = t2.customer_id_for_product_type
   ) as t
   where rank_count_craftsman = 1	
),
insert_delta as (
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday, 
        customer_email,
        customer_money, 
        platform_money, 
        count_orders, 
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        popular_craftsman_id,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period
    ) 
    select 
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_orders,
            avg_price_order,
            median_time_order_completed,
            top_product_category,
            popular_craftsman_id,
            count_order_created, 
            count_order_in_progress,
            count_order_delivery, 
            count_order_done, 
            count_order_not_done,
            report_period 
 	from dwh_delta_insert_result
),
update_delta as ( 
    update dwh.customer_report_datamart set
        customer_name = updates.customer_name, 
        customer_address = updates.customer_address, 
        customer_birthday = updates.customer_birthday, 
        customer_email = updates.customer_email, 
        customer_money = updates.customer_money, 
        platform_money = updates.platform_money, 
        count_orders = updates.count_orders, 
        avg_price_order = updates.avg_price_order, 
        median_time_order_completed = updates.median_time_order_completed, 
        top_product_category = updates.top_product_category,
        popular_craftsman_id = updates.popular_craftsman_id,
        count_order_created = updates.count_order_created, 
        count_order_in_progress = updates.count_order_in_progress, 
        count_order_delivery = updates.count_order_delivery, 
        count_order_done = updates.count_order_done,
        count_order_not_done = updates.count_order_not_done, 
        report_period = updates.report_period
    from (
        select 
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_orders,
            avg_price_order,
            median_time_order_completed,
            top_product_category,
            popular_craftsman_id,
            count_order_created,
            count_order_in_progress,
            count_order_delivery,
            count_order_done,
            count_order_not_done,
            report_period 
    	from dwh_delta_update_result
	) AS updates
    where dwh.customer_report_datamart.customer_id = updates.customer_id
),
insert_load_date AS (
    insert into dwh.load_dates_customer_report (
        load_dttm
    )
    select GREATEST(MAX(coalesce(craftsman_load_dttm, customers_load_dttm, products_load_dttm)), now())
   	from dwh_delta
)
select 'increment datamart';
