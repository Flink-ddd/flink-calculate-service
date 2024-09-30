package com.panda.flink;

//import com.panda.flink.config.TaskThreadPoolConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * @author muxiaohui
 */
@SpringBootApplication(scanBasePackages = "com.panda.flink")
//@EnableConfigurationProperties({TaskThreadPoolConfig.class})
public class FlinkServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkServiceApplication.class, args);
    }

}
