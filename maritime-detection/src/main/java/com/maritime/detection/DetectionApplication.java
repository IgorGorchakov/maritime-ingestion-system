package com.maritime.detection;

import com.maritime.detection.config.DetectionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(DetectionProperties.class)
public class DetectionApplication {
    public static void main(String[] args) {
        SpringApplication.run(DetectionApplication.class, args);
    }
}
