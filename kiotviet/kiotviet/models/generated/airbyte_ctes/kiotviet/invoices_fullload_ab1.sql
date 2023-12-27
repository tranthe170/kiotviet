{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_airbyte_kiotviet",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('kiotviet', '_airbyte_raw_invoices_fullload_airbytestorage') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','branchId'],['branchId,']) }} as BRANCHID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','branchName'],['branchName']) }} as BRANCHNAME,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','code'],['code']) }} as CODE,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','createdDate'],['createdDate']) }} as CREATEDDATE,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','customerCode'],['customerCode']) }} as CUSTOMERCODE,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','customerId'],['customerId']) }} as CUSTOMERID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','customerName'],['customerName']) }} as CUSTOMERNAME,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','id'],['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','invoiceDetails'],['invoiceDetails']) }} as INVOICEDETAILS,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','orderCode'],['orderCode']) }} as ORDERCODE,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','payments'],['payments']) }} as PAYMENTS,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','purchaseDate'],['purchaseDate']) }} as PURCHASEDATE,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','soldById'],['soldById']) }} as SOLDBYID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','soldByName'],['soldByName']) }} as SOLDBYNAME,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','status'],['status']) }} as STATUS,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','statusValue'],['statusValue']) }} as STATUSVALUE,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','total'],['total']) }} as TOTAL,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','totalPayment'],['totalPayment']) }} as TOTALPAYMENT,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','usingCod'],['usingCod']) }} as USINGCOD,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','uuid'],['uuid']) }} as UUID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','saleChannelId'],['saleChannelId']) }} as SALECHANNELID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','orderId'],['orderId']) }} as ORDERID,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','discount'],['discount']) }} as DISCOUNT,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','description'],['description']) }} as DESCRIPTION,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','discountRatio'],['discountRatio']) }} as DISCOUNTRATIO,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','invoiceOrderSurcharges'],['invoiceOrderSurcharges']) }} as INVOICEORDERSURCHARGES,
    {{ json_extract_scalar('_airbyte_data', ['_airbyte_data','modifiedDate'],['modifiedDate']) }} as MODIFIEDDATE,
    {{ json_extract_scalar('_airbyte_data', ['_ab_source_blob_name'],['_ab_source_blob_name']) }} as _AB_SOURCE_BLOB_NAME,
    {{ json_extract_scalar('_airbyte_data', ['_ab_source_file_last_modified'],['_ab_source_file_last_modified']) }} as _AB_SOURCE_FILE_LAST_MODIFIED,
    _airbyte_ab_id as _AIRBYTE_AB_ID,
    _airbyte_emitted_at as _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('kiotviet', '_airbyte_raw_invoices_fullload_airbytestorage') }} as table_alias
-- invoices
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

