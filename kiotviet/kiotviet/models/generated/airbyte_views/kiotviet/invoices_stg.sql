{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_airbyte_kiotviet",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('invoices_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'BRANCHID',
        'BRANCHNAME',
        'CODE',
        'CREATEDDATE',
        'CUSTOMERCODE',
        'CUSTOMERID',
        'CUSTOMERNAME',
        'ID',
        'INVOICEDETAILS',
        'ORDERCODE',
        'PAYMENTS',
        'PURCHASEDATE',
        'SOLDBYID',
        'SOLDBYNAME',
        'STATUS',
        'STATUSVALUE',
        'TOTAL',
        'TOTALPAYMENT',
        'USINGCOD',
        'UUID',
        'SALECHANNELID',
        'ORDERID',
        'DISCOUNT',
        'DESCRIPTION',
        'DISCOUNTRATIO',
        'INVOICEORDERSURCHARGES',
        'MODIFIEDDATE',
    ]) }} as _AIRBYTE_INVOICES_HASHID,
    tmp.*
from {{ ref('invoices_ab2') }} tmp
-- invoices
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

