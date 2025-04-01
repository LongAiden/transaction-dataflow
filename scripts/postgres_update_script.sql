UPDATE airflow.public.transaction_data
SET "Sources" = 'Current Account'
WHERE "Vendor" = 'Sending Out'
 AND "Sources" = 'Credit Card';

select * from airflow.public.transaction_data td 
limit 10