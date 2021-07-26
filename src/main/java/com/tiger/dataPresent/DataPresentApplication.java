package com.tiger.dataPresent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceAutoConfigure;

@SpringBootApplication
@ComponentScan(basePackages={"com.tiger.*"})
@ServletComponentScan
@EnableScheduling
@EnableAutoConfiguration(exclude = { DruidDataSourceAutoConfigure.class,DataSourceAutoConfiguration.class})
public class DataPresentApplication {
	public static void main(String[] args) {
		SpringApplication.run(DataPresentApplication.class, args);
	}

}
