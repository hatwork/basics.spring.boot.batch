package com.hatim.basics.spring.boot.batch.comp;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Component
public class BatchComponent {
	
	@Bean
	public Step getStep1(StepBuilderFactory stepBuilderFactory) {
		return stepBuilderFactory.get("Step1").<String, String>chunk(200).reader(getReader()).writer(getWriter()).processor(getProcessor()).build();
	}

	@Bean
	public Step getStep2(StepBuilderFactory stepBuilderFactory) {
		return stepBuilderFactory.get("Step2").<String, String>chunk(200).reader(getFileReader()).writer(getWriter()).processor(getProcessor()).build();
	}

	@Bean
	public Step getStep3(StepBuilderFactory stepBuilderFactory) {
		IncrmntBean bean = new IncrmntBean();
		return stepBuilderFactory.get("Step3").<String, String>chunk(200).reader(()->{
			bean.increment();
			if( bean.getVal() > 10 ) {
				return null;
			}
			return "Step3: Line " + bean.getVal();
		}).writer((List<? extends String> items) ->{
			for (String string : items) {
				System.out.println(string);
			}
		} ).build();
	}

	
	@Bean
	private Flow getFlow(StepBuilderFactory stepBuilderFactory) {
		return new FlowBuilder<Flow>("Test").start(getStep2(stepBuilderFactory)).next(getStep3(stepBuilderFactory)).build();
	}

	@Bean
	public Job getJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
		return jobBuilderFactory.get("Job1").incrementer(new RunIdIncrementer()).start(getFlow(stepBuilderFactory)).next(getStep1(stepBuilderFactory)).end().build();
	}

	@Bean
	private FlatFileItemReader<String> getFileReader() {
		return new FlatFileItemReaderBuilder<String>().name("Read-File").resource(new ClassPathResource("abc.txt")).lineMapper(new DefaultLineMapper<String>() {
			@Override
			public String mapLine(String line, int lineNumber) throws Exception {
				System.out.println(lineNumber + "  " + line);
				return line;
			}
		}).build();
	}
	
	@Bean
	public ItemReader<String> getReader() {
		return new ItemReader<String>() {
			private long counter = 0;
			@Override
			public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
				
				if( counter > 10 ) {
					return null;
				}
				
				return "Step1 : Line " + ++counter;
			}
		};
	}
	
	@Bean
	public ItemWriter<String> getWriter() {
		return new ItemWriter<String>() {
			
			@Override
			public void write(List<? extends String> items) throws Exception {
				for (String string : items) {
					System.out.println(string);
				}
			}
		};
	}
	
	@Bean
	public ItemProcessor<String, String> getProcessor() {
		return new ItemProcessor<String, String>() {
			@Override
			public String process(String item) throws Exception {
				return null != item ? item.toUpperCase() : "NULL";
			}
		};
	}
	
	
	class IncrmntBean {
		private int i = 0;
		public void increment() {
			i++;
		}
		public int getVal() {
			return i;
		}
	}
}
