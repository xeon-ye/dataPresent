package com.tiger.dataPresent.utils;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Configuration
@Component
public class StartQuartzJobListener implements ApplicationListener<ContextRefreshedEvent> {
	private static Logger log = LoggerFactory.getLogger(StartQuartzJobListener.class);
	@Autowired
    private QuartzManager quartzManager;

    /**
     * 初始启动quartz
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
    	quartzManager.scheduleTask();
        quartzManager.start();
        log.info("定时任务已经启动...");
    }
}
