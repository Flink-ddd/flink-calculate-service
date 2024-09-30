package com.panda.flink.business.order;

import lombok.Data;

/**
 * @author muxiaohui
 */
@Data
public class Order implements java.io.Serializable{
    private String orderId;
    private String userName;
    private String gender;
    private String goods;
    private String goodsType;
    private String brand;
    private String orderTime;
    private Long orderTimeSeries;
    private Double price;
    private Integer num;
    private Double totalPrice;
    private String status;
    private String address;
    private Long phone;

    public Order(String orderId, String userName, String gender, String goods, String goodsType, String brand, String orderTime, Long orderTimeSeries,
                 Double price, Integer num, Double totalPrice, String status, String address, Long phone) {
        this.orderId = orderId;
        this.userName = userName;
        this.gender = gender;
        this.goods = goods;
        this.goodsType = goodsType;
        this.brand = brand;
        this.orderTime = orderTime;
        this.orderTimeSeries = orderTimeSeries;
        this.price = price;
        this.num = num;
        this.totalPrice = totalPrice;
        this.status = status;
        this.address = address;
        this.phone = phone;
    }

    public Order(){

    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getGoods() {
        return goods;
    }

    public void setGoods(String goods) {
        this.goods = goods;
    }

    public String getGoodsType() {
        return goodsType;
    }

    public void setGoodsType(String goodsType) {
        this.goodsType = goodsType;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public Long getOrderTimeSeries() {
        return orderTimeSeries;
    }

    public void setOrderTimeSeries(Long orderTimeSeries) {
        this.orderTimeSeries = orderTimeSeries;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public Double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(Double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Long getPhone() {
        return phone;
    }

    public void setPhone(Long phone) {
        this.phone = phone;
    }
}
