package com.paicbd.smsc.smsf.mt;

import com.paicbd.smsc.utils.Generated;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@Generated
@EnableScheduling
@SpringBootApplication
public class SmsfMtApplication {

    public static void main(String[] args) {
        SpringApplication.run(SmsfMtApplication.class, args);
    }
}
