-- models/cohort_txn_profit.sql

{{ config (materialized='table') }}

WITH cohort_data AS (
    SELECT
        DENSE_RANK() OVER (ORDER BY date_trunc('month', wd.wallet_createdat_utc2)) AS month_key,
        to_char(date_trunc('month', wd.wallet_createdat_utc2), 'MON YYYY') as cohort_month,
        tft.walletdetailsid,
        replace(substring(wd.wallet_status,14) ,'_',' ') AS wallet_status,
        pd.partner_name,
        pd.profile_type,
        tft.is_employee,
        tft.txndetailsid,
		tft.transaction_modifiedat_utc2,
		dd.full_date,
		tid.full_time,
        td.txntype,
        tft.amount,
        tft.total_revenue_before_vat,
        tft.total_cost_before_vat,
        (tft.total_revenue_before_vat - tft.total_cost_before_vat) as profit_before_vat 
    
    FROM {{ source('dbt-fact', 'transactions_fact') }} tft
    JOIN {{ source('dbt-dimensions', 'wallets_dimension') }} wd
        ON wd.walletid = tft.walletdetailsid
    JOIN {{ source('dbt-dimensions', 'profiles_dimension') }} pd
        ON pd.walletprofileid = tft.walletprofileid
    JOIN {{ source('dbt-dimensions', 'transactions_dimension') }} td
        ON td.txndetailsid = tft.txndetailsid
    JOIN {{ source('dbt-dimensions', 'date-dimension')}} dd
        ON tft.date_id = dd.date_id
    JOIN {{ source('dbt-dimensions', 'time-dimension')}} tid
        ON tft.time_id = tid.time_id
    WHERE wd.wallet_type = 'WalletType_CONSUMER'
        AND td.transactionstatus IN ('TransactionStatus_POSTED', 'TransactionStatus_POSTED_FAWRY','TransactionStatus_PENDING_ADVICE')
)

SELECT * FROM cohort_data