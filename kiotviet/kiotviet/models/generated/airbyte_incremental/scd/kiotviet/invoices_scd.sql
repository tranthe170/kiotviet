{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    schema = "kiotviet",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='invoices'
                        )
                    %}
                    {#
                    If the final table doesn't exist, then obviously we can't delete anything from it.
                    Also, after a reset, the final table is created without the _airbyte_unique_key column (this column is created during the first sync)
                    So skip this deletion if the column doesn't exist. (in this case, the table is guaranteed to be empty anyway)
                    #}
                    {%
                    if final_table_relation is not none and '_AIRBYTE_UNIQUE_KEY' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active:
                    -- This query is equivalent, but the left join version is more performant:
                    -- delete from final_table where unique_key in (
                    --     select unique_key from scd_table where 1 = 1 <incremental_clause(normalized_at, final_table)>
                    -- ) and unique_key not in (
                    --     select unique_key from scd_table where active_row = 1 <incremental_clause(normalized_at, final_table)>
                    -- )
                    -- We're incremental against normalized_at rather than emitted_at because we need to fetch the SCD
                    -- entries that were _updated_ recently. This is because a deleted record will have an SCD record
                    -- which was emitted a long time ago, but recently re-normalized to have active_row = 0.
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._AIRBYTE_UNIQUE_KEY in (
                        select recent_records.unique_key
                        from (
                                select distinct _AIRBYTE_UNIQUE_KEY as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('invoices')) }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('invoices')) }}
                                group by _AIRBYTE_UNIQUE_KEY
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","drop view _airbyte_kiotviet.invoices_stg"],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('invoices_stg')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('invoices_stg')  }}
    -- INVOICES from {{ source('kiotviet', '_airbyte_raw_invoices_airbytestorage') }}
    where 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids as (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'CREATEDDATE',
            'ID',
            
        ]) }} as _AIRBYTE_UNIQUE_KEY
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('invoices_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('invoices_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('invoices_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('invoices_stg')  }}
    -- INVOICES from {{ source('kiotviet', '_airbyte_raw_invoices_airbytestorage') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      'CREATEDDATE',
      'ID',
      ]) }} as _AIRBYTE_UNIQUE_KEY,
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
      CREATEDDATE as _AIRBYTE_START_AT,
      lag(CREATEDDATE) over (
        partition by CREATEDDATE, ID
        order by
            CREATEDDATE is null asc,
            CREATEDDATE desc,
            _AIRBYTE_EMITTED_AT desc
      ) as _AIRBYTE_END_AT,
      case when row_number() over (
        partition by CREATEDDATE, ID
        order by
            CREATEDDATE is null asc,
            CREATEDDATE desc,
            _AIRBYTE_EMITTED_AT desc
      ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
      _AIRBYTE_AB_ID,
      _AIRBYTE_EMITTED_AT,
      _AB_SOURCE_BLOB_NAME,
      _AB_SOURCE_FILE_LAST_MODIFIED,
      _AIRBYTE_INVOICES_HASHID
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _AIRBYTE_UNIQUE_KEY,
                _AIRBYTE_START_AT,
                _AIRBYTE_EMITTED_AT
            order by _AIRBYTE_ACTIVE_ROW desc, _AIRBYTE_AB_ID
        ) as _AIRBYTE_ROW_NUM,
        {{ dbt_utils.surrogate_key([
          '_AIRBYTE_UNIQUE_KEY',
          '_AIRBYTE_START_AT',
          '_AIRBYTE_EMITTED_AT'
        ]) }} as _AIRBYTE_UNIQUE_KEY_SCD,
        scd_data.*
    from scd_data
)
select
    _AIRBYTE_UNIQUE_KEY,
    _AIRBYTE_UNIQUE_KEY_SCD,
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
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_INVOICES_HASHID
from dedup_data where _AIRBYTE_ROW_NUM = 1

