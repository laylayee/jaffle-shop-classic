
-- ==============================================================
--   Job Name                      : dwd_order_sum.sql
--   DataLayer/SubjectArea/Domain  : /DWD/ORDER/PAYMENT
--   Creator                       : layla.ye
--   Creation Date                 : 2025/07/02
--   Execution Cycle               : Daily
--   Source Table                  : stg_orders  理财账户明细
--   Target Table                  : stg_orders  客户管理资产负债汇总
--   Version Information           : V1.0
--   Deployment Path               : /Users/layla.ye/PycharmProjects/jaffle-shop-classic/models/layla_test_db/dwd_order_sum.sql
--   Process Management            : 如有业务需求,填写对应需求详细路径和名称；若无明确业务需求，填写详细设计文档路径及名称
--   Function Desc                 : 协议主题_协议关系历史表加工：0、账户表账号与客户号，1、账户表账号与卡号，2、账户表账号与凭证号，3、账户表账号与老账号，4、账户表账号与账号，5、表外帐号对应关系，6、信用卡号对应关系
--   Key Notes                     : 特殊注意事项说明
-- ==============================================================
--   Modification History（仅保留最新5个版本说明或大版本）：
--   Version  Modified_date       Modified_By         Description
--   V1.1     2025-08-08          layla.ye            add：0账号与客户关系
--   V1.2     2025-08-12          layla.ye            update：
-- ==============================================================

{{
  config(
    tags = 'daily',
    materialized = 'table',
    schema = 'layla_test_db',
    unique_key = ['order_id']
   )
}}


{% set payment_methods = ['credit_card', 'coupon'] %}

select
     o.order_id                                         as order_id             -- 订单ID
    ,o.customer_id                                      as cust_id              -- 客户ID
    ,o.order_date                                       as order_date           -- 订单日期
    ,o.status                                           as order_status         -- 订单状态
    {% for method in payment_methods -%}
    ,sum(case when p.payment_method = '{{ method }}'
              then p.amount else 0 end)                 as {{ method }}_amount  -- 支付方式金额
    {% endfor -%}
    ,sum(p.amount)                                      as total_amount         -- 总支付金额
from {{ ref('stg_orders') }} o
left join {{ ref('stg_payments') }} p on o.order_id = p.order_id
group by 1,2,3,4