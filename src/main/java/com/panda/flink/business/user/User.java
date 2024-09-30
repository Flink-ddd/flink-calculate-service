package com.panda.flink.business.user;

import lombok.Data;

/**
 * @author muxiaohui
 */
@Data
public class User {
    private String userId;
    private String userName;
    private String gender;
    private String address;
    private Long phone;
    private String age;
    private String registerTime;
    private Integer registerLabel;
    private String registerSource;
    private Long registerTimeSeries;

    public User(String userId, String userName, String gender, String address, Long phone, String age, String registerTime, Integer registerLabel, String registerSource, Long registerTimeSeries) {
        this.userId = userId;
        this.userName = userName;
        this.gender = gender;
        this.address = address;
        this.phone = phone;
        this.age = age;
        this.registerLabel = registerLabel;
        this.registerSource = registerSource;
        this.registerTime = registerTime;
        this.registerTimeSeries = registerTimeSeries;
    }
}
