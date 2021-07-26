package com.tiger.utils;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;

import com.alibaba.fastjson.JSONObject;

@Component
public class FilterForCors implements Filter{
	private static Logger log = LoggerFactory.getLogger(FilterForCors.class);
    @Autowired
    private Environment environment;
	@Override
    public void init(FilterConfig filterConfig) throws ServletException {
 
    }
 
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest)servletRequest;
        HttpServletResponse response =(HttpServletResponse) servletResponse;
        System.out.println(request.getMethod());
        String allowHeaders = environment.getProperty("allowHeaders","Content-type,APP_TOKEN");
        response.setHeader("Access-Control-Allow-Origin", request.getHeader("Origin"));
        response.setHeader("Access-Control-Allow-Methods", "*");
        response.setHeader("Access-Control-Allow-Headers", allowHeaders);
        response.setHeader("Access-Control-Allow-Credentials","true"); // 允许携带验证信息
        response.setHeader("Vary", "Origin");
        if (RequestMethod.OPTIONS.name().equals(request.getMethod().toUpperCase())) {
            log.info("OPTIONS请求不做拦截操作");
            return;
        }
        filterChain.doFilter(servletRequest, servletResponse);
		return;
    }
 
    @Override
    public void destroy() {
 
    }
}
