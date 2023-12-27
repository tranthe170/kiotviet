{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_airbyte_kiotviet",
    tags = [ "top-level-integerermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the json schema type
-- depends_on: {{ ref('invoices_ab1') }}
select
    cast(BRANCHID as integer) as BRANCHID,
    cast(BRANCHNAME as text) as BRANCHNAME,
    cast(CODE as text) as CODE,
    cast(CREATEDDATE as text) as CREATEDDATE,
    cast(CUSTOMERCODE as text) as CUSTOMERCODE,
    cast(CUSTOMERID as integer) as CUSTOMERID,
    cast(CUSTOMERNAME as text) as CUSTOMERNAME,
    cast(ID as integer) as ID,
    cast(INVOICEDETAILS as json) as INVOICEDETAILS,
    cast(ORDERCODE as text) as ORDERCODE,
    cast(PAYMENTS as json) as PAYMENTS,
    cast(PURCHASEDATE as text) as PURCHASEDATE,
    cast(SOLDBYID as integer) as SOLDBYID,
    cast(SOLDBYNAME as text) as SOLDBYNAME,
    cast(STATUS as text) as STATUS,
    cast(STATUSVALUE as text) as STATUSVALUE,
    cast(TOTAL as numeric) as TOTAL,
    cast(TOTALPAYMENT as numeric) as TOTALPAYMENT,
    cast(USINGCOD as bool) as USINGCOD,
    cast(UUID as text) as UUID,
    cast(SALECHANNELID as integer) as SALECHANNELID,
    cast(ORDERID as text) as ORDERID,
    cast(DISCOUNT as numeric) as DISCOUNT,
    cast(DESCRIPTION as text) as DESCRIPTION,
    cast(DISCOUNTRATIO as text) as DISCOUNTRATIO,
    cast(INVOICEORDERSURCHARGES as json) as INVOICEORDERSURCHARGES,
    CASE WHEN MODIFIEDDATE IS NOT NULL THEN cast(MODIFIEDDATE as text) 
    ELSE cast(CREATEDDATE as text) 
    END as MODIFIEDDATE,
    cast(_AB_SOURCE_BLOB_NAME as text) as _AB_SOURCE_BLOB_NAME,
    cast(_AB_SOURCE_FILE_LAST_MODIFIED as text) as _AB_SOURCE_FILE_LAST_MODIFIED,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from
    ( select 
        * 
      from {{ ref('invoices_ab1') }}
      union all
      select
        *
       from {{ ref('invoices_fullload_ab1') }} 
    ) t1
-- invoices
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

