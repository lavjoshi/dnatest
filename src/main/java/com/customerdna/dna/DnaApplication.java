package com.customerdna.dna;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
//@ComponentScan(basePackages = "com.customerdna.dna")
public class DnaApplication {

	public static void main(String[] args) {
		SpringApplication.run(DnaApplication.class, args);
	}
}
