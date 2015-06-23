 select 
        format(stime,'yyyy-MM-dd') tdate,
        app_id,
        channel_id,
        count_distinct(device_id)  active_device_idcnt,
        bf_count_distinct('device_' + device_id,'st_app_channel_bf',format(stime,'yyyyMMdd')) active_device_idcnt,
        bf_count_distinct('account_' + account_id,'st_app_channel_bf',format(stime,'yyyyMMdd')) active_account_idcnt,
        -- bf_count_distinct('recharge_acccount_' + CBC(recharge > 0,account_id,''), 'st_app_channel_bf',format(stime,'yyyyMMdd')) recharge_account_idcnt,
        -- bf_count_distinct('recharge_device_' + IF(recharge > 0,device_id,null), 'st_app_channel_bf',format(stime,'yyyyMMdd')) recharge_device_idcnt,
        sum(recharge) recharge,
        ifnull(device_id,account_id)
into dw_app
from ods_app
where
        app_id != null and channel_id != null
group by 
        format(stime,'yyyy-MM-dd'),app_id,channel_id
        