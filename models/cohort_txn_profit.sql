-- models/cohort_txn_profit.sql

{{ config (materialized = 'materialized_view',
    on_configuration_change = 'apply') }}

SELECT
    DENSE_RANK() OVER (ORDER BY date_trunc('month', wd.wallet_createdat_local)) AS month_key,
    to_char(date_trunc('month', wd.wallet_createdat_local), 'MON YYYY') as cohort_month,
    wd.walletid,
    replace(substring(wd.wallet_status,14) ,'_',' ') AS wallet_status,
    pd.partner_name_en,
    pd.partner_name_ar,
    pd.profile_type,
    tf.is_employee,
    td.txndetailsid,
    td.transaction_modifiedat_local,
    ddm.full_date,
    tidm.full_time,
    td.txntype,
    tf.amount,
    tf.total_revenue_before_vat,
    tf.total_cost_before_vat,
    (tf.total_revenue_before_vat - tf.total_cost_before_vat) as profit_before_vat 

FROM {{ source('dbt-facts', 'transactions_fact') }} tf
JOIN {{ source('dbt-dimensions', 'wallets_dimension') }} wd
    ON wd.id = tf.wallet_key
JOIN {{ source('dbt-dimensions', 'profiles_dimension') }} pd
    ON pd.id = tf.profile_key
JOIN {{ source('dbt-dimensions', 'transactions_dimension') }} td
    ON td.id = tf.txn_key
JOIN {{ source('dbt-dimensions', 'date_dimension')}} ddm
    ON tf.date_txn_modified_key = ddm.date_id
JOIN {{ source('dbt-dimensions', 'time_dimension')}} tidm
    ON tf.time_txn_modified_key = tidm.time_id
WHERE wd.wallet_type = 'WalletType_CONSUMER'
    AND td.transactionstatus IN ('TransactionStatus_POSTED', 'TransactionStatus_POSTED_FAWRY','TransactionStatus_PENDING_ADVICE')

