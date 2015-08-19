package com.bigdata.elasticsearch.service;

import java.io.File;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Run this to load data into Mongodb using Spring Integration and ActiveMQ.
 */
public class LoadDataDriver {

    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
                "applicationContext.xml");
        DataLoader loader = ctx.getBean(DataLoader.class);

        // --------------------------------
        // load data - point this to your path
        // --------------------------------
        loader.loadData(new File("/Users/mathew/temp/P00000001-VA.csv"));

        // --------------------------------
        // print total count for verification
        // --------------------------------
        System.out.println("Total Count of Documents = "
                + loader.getTotalCount());

        //
        ctx.close();
    }
}
