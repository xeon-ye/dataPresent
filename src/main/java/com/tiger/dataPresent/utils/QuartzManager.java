package com.tiger.dataPresent.utils;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.tiger.dataPresent.service.PublicDataService;
import com.tiger.dataPresent.utils.job.ExportJob;
import com.tiger.utils.MultilDataSources;

@Component("quartzManager")
public class QuartzManager{
	private static Logger log = LoggerFactory.getLogger(QuartzManager.class);
	@Autowired
	private MultilDataSources multilDataSources;
	@Autowired
	private Scheduler scheduler;
	
	public void scheduleTask(){
		//周期性取数的申请。
		JdbcTemplate jdbcTemplate= multilDataSources.getPrimaryJdbcTemplate();
		StringBuffer sql = new StringBuffer("select to_char(id) aid,timer from etl_apply where ismultiple=1 and approved=1 ");
		List apps = jdbcTemplate.queryForList(sql.toString());
		if(apps!=null){
			for(int i=0;i<apps.size();i++){
				Map row = (Map)apps.get(i);
				String aid = (String)row.get("aid");
				String cron =(String)row.get("timer");
				if(StringUtils.isEmpty(cron)){
					log.error("周期性取数申请，但未设置时间表达式。申请ID:"+aid);
					continue;
				}
				JobDetail jcDt = JobBuilder.newJob(ExportJob.class)
					      .withIdentity("job_export_"+aid, "export")
					      .build();
				jcDt.getJobDataMap().put("aid", aid);
				Trigger trigger = null;
				try{
					trigger = TriggerBuilder.newTrigger().withIdentity("tg_export_"+aid, "export")
						.withSchedule(CronScheduleBuilder.cronSchedule(cron)).build(); 
				}catch(Exception e){
					log.error("触发器时间配置格式错误，任务类型：导出数据"+",申请id："+aid+",时间表达式："+cron);
				}
				try{
					scheduler.scheduleJob(jcDt, trigger);
				}catch(Exception e){
					log.error("加入调度任务失败"+",申请id："+aid+",时间表达式："+cron);
				}
			}
		}
	}
	public void start(){
		try{
			scheduler.start();
		} catch (SchedulerException se) {
            se.printStackTrace();
        }
	}
	public void shutdown(){
		try{
			scheduler.shutdown(true);
		} catch (SchedulerException se) {
            se.printStackTrace();
        }	
	}
	public void removeJob(String jobName, String groupName) {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, groupName);
        try {
            if (checkExists(jobName, groupName)) {
                scheduler.pauseTrigger(triggerKey);
                scheduler.unscheduleJob(triggerKey);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	public boolean checkExists(String jobName, String groupName) throws SchedulerException {
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, groupName);
        return scheduler.checkExists(triggerKey);
    }
	public void updateJob(String jobName, String groupName, String time){
        TriggerKey triggerKey = TriggerKey.triggerKey(jobName, groupName);
        try{
            int fixedRate = Integer.valueOf(time)*1000;
            SimpleTriggerImpl trigger = (SimpleTriggerImpl)scheduler.getTrigger(triggerKey);
            if(trigger.getRepeatInterval() != fixedRate){
                SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.repeatSecondlyForever(fixedRate);
                Trigger newTrigger = TriggerBuilder.newTrigger().withIdentity(jobName, groupName)
                        .withSchedule(scheduleBuilder).build();

                removeJob(jobName, groupName);
                scheduler.rescheduleJob(triggerKey, newTrigger);
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
