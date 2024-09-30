package com.panda.flink.common;


/**
 * @author muxh
 */
public interface DomainFactory<V extends VO, D extends DomainI> {
    D perfect(V var1);

    static <T> T get(Class<T> clazz) {
        return ApplicationContextHelper.getBean(clazz);
    }
}
