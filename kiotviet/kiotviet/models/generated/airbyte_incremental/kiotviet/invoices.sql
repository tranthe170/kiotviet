{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    schema = "kiotviet",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('invoices_scd') }}
select
    _AIRBYTE_UNIQUE_KEY,
    BRANCHID,
    BRANCHNAME,
    CODE,
    CREATEDDATE,
    CUSTOMERCODE,
    CUSTOMERID,
    CUSTOMERNAME,
    ID,
    INVOICEDETAILS,
    ORDERCODE,
    PAYMENTS,
    PURCHASEDATE,
    SOLDBYID,
    SOLDBYNAME,
    STATUS,
    STATUSVALUE,
    TOTAL,
    TOTALPAYMENT,
    USINGCOD,
    UUID,
    SALECHANNELID,
    ORDERID,
    DISCOUNT,
    DESCRIPTION,
    DISCOUNTRATIO,
    INVOICEORDERSURCHARGES,
    MODIFIEDDATE,
    _AB_SOURCE_BLOB_NAME,
    _AB_SOURCE_FILE_LAST_MODIFIED,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_INVOICES_HASHID
from {{ ref('invoices_scd') }}
-- invoices from {{ source('kiotviet', '_airbyte_raw_invoices_airbytestorage') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

