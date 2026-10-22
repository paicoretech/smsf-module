package com.paicbd.smsc.smsf.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class SmsfModuleApplicationTests {

    @Test
    void mainClassIsPresent() {
        assertNotNull(SmsfCoreApplication.class.getName());
    }
}
