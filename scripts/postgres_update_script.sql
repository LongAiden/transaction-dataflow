UPDATE airflow.public.transaction_data
SET "Sources" = 'Current Account'
WHERE "Vendor" = 'Sending Out'
 AND "Sources" = 'Credit Card';

SELECT * 
FROM airflow.public.transaction_data
LIMIT 10