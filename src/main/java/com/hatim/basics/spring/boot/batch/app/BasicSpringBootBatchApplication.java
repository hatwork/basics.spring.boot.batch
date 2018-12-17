package com.hatim.basics.spring.boot.batch.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.hatim.basics.spring.boot.batch")
//@EnableBatchProcessing
public class BasicSpringBootBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(BasicSpringBootBatchApplication.class, args);
	}

}
