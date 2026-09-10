package com.example.BuildingKafkaFromScratch;

import com.example.BuildingKafkaFromScratch.broker.SimpleKafkaBroker;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Path;

@SpringBootApplication
public class BuildingKafkaFromScratchApplication implements CommandLineRunner {

	public static void main(String[] args) {

		SpringApplication.run(
				BuildingKafkaFromScratchApplication.class,
				args
		);
	}

	@Override
	public void run(String... args)
			throws Exception {

		SimpleKafkaBroker broker =
				new SimpleKafkaBroker(
						Path.of("data")
				);

		broker.start(9092);
	}
}
