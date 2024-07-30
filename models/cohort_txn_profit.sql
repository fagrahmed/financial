-- models/cohort_txn_profit.sql

{{ config (materialized = 'materialized_view',
    on_configuration_change = 'apply') }}

SELECT
    DENSE_RANK() OVER (ORDER BY date_trunc('month', wd.wallet_createdat_local)) AS month_key,
    to_char(date_trunc('month', wd.wallet_createdat_local), 'MON YYYY') as cohort_month,
    wd.walletid,
    replace(substring(wd.wallet_status,14) ,'_',' ') AS wallet_status,
    pd.partner_name,
    pd.profile_type,
    tft.is_employee,
    tft.txndetailsid,
    tft.transaction_modifiedat_local,
    ddm.full_date,
    tidm.full_time,
    td.txntype,
    tft.amount,
    tft.total_revenue_before_vat,
    tft.total_cost_before_vat,
    (tft.total_revenue_before_vat - tft.total_cost_before_vat) as profit_before_vat 

FROM {{ source('dbt-facts', 'transactions_fact') }} tft
JOIN {{ source('dbt-dimensions', 'wallets_dimension') }} wd
    ON wd.id = tft.wallet_key
JOIN {{ source('dbt-dimensions', 'profiles_dimension') }} pd
    ON pd.id = tft.profile_key
JOIN {{ source('dbt-dimensions', 'transactions_dimension') }} td
    ON td.txndetailsid = tft.txndetailsid
JOIN {{ source('dbt-dimensions', 'date_dimension')}} ddm
    ON tft.date_txn_modified_key = ddm.date_id
JOIN {{ source('dbt-dimensions', 'time_dimension')}} tidm
    ON tft.time_txn_modifed_key = tidm.time_id
WHERE wd.wallet_type = 'WalletType_CONSUMER'
    AND td.transactionstatus IN ('TransactionStatus_POSTED', 'TransactionStatus_POSTED_FAWRY','TransactionStatus_PENDING_ADVICE')

