package com.example.BuildingKafkaFromScratch.protocol;

public enum ApiKey {
    CREATE_TOPIC(1),
    PRODUCE(2),
    FETCH(3);

    private final short id;

    ApiKey(int id){
        this.id = (short) id;
    }
    public short id(){
        return id;
    }

    public static ApiKey fromId(short id){
        for(ApiKey apiKey : values()){
            if(apiKey.id == id){
                return apiKey;
            }
        }
        throw new IllegalArgumentException(
                "Unknown API key: "+id
        );
    }
}
