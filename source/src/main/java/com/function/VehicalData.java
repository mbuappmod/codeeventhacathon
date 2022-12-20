package com.function;

public class VehicalData{

    private String timestamp;
    private String deviceId;
    private String count;


    public String getTimestamp(){
        return this.timestamp;
        
    }

     public String getDeviceId(){
        return this.deviceId;
        
    }

 public String getCount(){
        return this.count;
        
    }


    public void setTimestamp(String timestamp){
        this.timestamp = timestamp;
    }

    public void setDeviceId(String deviceId){
        this.deviceId = deviceId;
    }

    public void setCount(String count){
        this.count = count;
    }

    public String toString (){
        return "device Id :: "  +deviceId + "  time stamp :::::" +timestamp + "   count :::::" +count; 
    }


}