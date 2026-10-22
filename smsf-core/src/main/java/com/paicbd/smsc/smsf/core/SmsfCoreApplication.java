package com.paicbd.smsc.smsf.core;

import com.paicbd.smsc.utils.Generated;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@Generated
@SpringBootApplication(exclude = {
        ServletWebServerFactoryAutoConfiguration.class,
        DispatcherServletAutoConfiguration.class,
        WebMvcAutoConfiguration.class,
        ErrorMvcAutoConfiguration.class
})
public class SmsfCoreApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(SmsfCoreApplication.class)
                .web(WebApplicationType.REACTIVE)
                .run(args);
    }
}
