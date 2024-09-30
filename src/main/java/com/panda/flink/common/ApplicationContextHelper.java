package com.panda.flink.common;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @author muxh
 */

@Component
public class ApplicationContextHelper implements ApplicationContextAware {
    private static ApplicationContext applicationContext;
    private static BeanDefinitionRegistry beanDefinitonRegistry;

    public ApplicationContextHelper() {
    }

    public static synchronized void registerBean(String beanName, Class clazz) {
        if (null != beanName && null != clazz) {
            BeanDefinition beanDefinition = getBeanDefinitionBuilder(clazz).getBeanDefinition();
            if (!beanDefinitonRegistry.containsBeanDefinition(beanName)) {
                beanDefinitonRegistry.registerBeanDefinition(beanName, beanDefinition);
            }

        } else {
            throw new RuntimeException(beanName + "注册失败");
        }
    }

    private static BeanDefinitionBuilder getBeanDefinitionBuilder(Class clazz) {
        return BeanDefinitionBuilder.genericBeanDefinition(clazz);
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        ApplicationContextHelper.applicationContext = applicationContext;
        ConfigurableApplicationContext configurableApplicationContext = (ConfigurableApplicationContext) applicationContext;
        beanDefinitonRegistry = (BeanDefinitionRegistry) configurableApplicationContext.getBeanFactory();
    }

    public static <T> T getBean(Class<T> targetClz) {
        Object beanInstance = null;

        try {
            beanInstance = applicationContext.getBean(targetClz);
        } catch (Exception var3) {
        }

        if (beanInstance == null) {
            String simpleName = targetClz.getSimpleName();
            simpleName = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
            beanInstance = applicationContext.getBean(simpleName);
        }

        if (beanInstance == null) {
            throw new RuntimeException("beanName " + targetClz.getSimpleName() + " can not be found in ApplicationContext (byType and byName)");
        } else {
            return (T) beanInstance;
        }
    }
}
