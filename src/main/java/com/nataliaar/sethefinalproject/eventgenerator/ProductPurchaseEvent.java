package com.nataliaar.sethefinalproject.eventgenerator;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ProductPurchaseEvent {

    private String productName;
    private Double productPrice;
    private LocalDateTime purchaseDate;
    private String productCategory;
    private String clientIpAddress;

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Double getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(Double productPrice) {
        this.productPrice = productPrice;
    }

    public LocalDateTime getPurchaseDate() {
        return purchaseDate;
    }

    public void setPurchaseDate(LocalDateTime purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }

    public String getClientIpAddress() {
        return clientIpAddress;
    }

    public void setClientIpAddress(String clientIpAddress) {
        this.clientIpAddress = clientIpAddress;
    }

    @Override
    public String toString() {
        return "ProductPurchaseEvent [productName=" + productName + ", productPrice=" + productPrice + ", purchaseDate="
            + purchaseDate + ", productCategory=" + productCategory + ", clientIpAddress=" + clientIpAddress + "]";
    }

    public String[] toStringArray() {
        return new String[] {
            productName, 
            productPrice.toString(),
            purchaseDate.format(DateTimeFormatter.ISO_DATE_TIME), 
            productCategory, 
            clientIpAddress
        };
    }
}
