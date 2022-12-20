package com.function;

import java.time.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

/**
 * Azure Functions with Timer trigger.
 */
public class TimerTriggerJava {
     /**
     * This function will be invoked periodically according to the specified schedule.
     */

     private CosmosClient client;

    private final String databaseName = "SmartCityDb";
    private final String containerName = "SmartCityDbInfo";
    private final String documentLastName = "Witherspoon";

    private CosmosDatabase cosmosDatabase;
    private CosmosContainer cosmosContainer;


    public static String MASTER_KEY =
                            "d1RYgaRQM2sKBhICPbPeAq4WnycXCj5z91MYHDqVvOuhDcGfYwbWzjlRmtOUNPavcyIsiCIJTk9iACDblg4ohA==";

    public static String HOST =  "https://smartcity-cosmosdb.documents.azure.com:443/";

     private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();

    @FunctionName("TimerTriggerJava")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "0 */1 * * * *") String timerInfo,
        final ExecutionContext context
    ) {
        
        context.getLogger().info("Java Timer trigger function executed at: " + LocalDateTime.now());

          List<VehicalData> todoItems = new ArrayList<VehicalData>();

        client = new CosmosClientBuilder()
                .endpoint(HOST)
                .key(MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildClient();
        String sql = "SELECT * FROM c";
         int maxItemCount = 1000;
        int maxDegreeOfParallelism = 1000;
        int maxBufferedItemCount = 100;

          String continuationToken = null;

          
        do {

            for (FeedResponse<JsonNode> pageResponse :
                getContainerCreateResourcesIfNotExist(client).queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class)
                    .iterableByPage(continuationToken, maxItemCount)) {

                continuationToken = pageResponse.getContinuationToken();

                for (JsonNode item : pageResponse.getElements()) {

                    try {
                        todoItems.add(OBJECT_MAPPER.treeToValue(item, VehicalData.class));
                    } catch (JsonProcessingException e) {
                       e.printStackTrace();
                    }

                }
            }

        } while (continuationToken != null);
        int carCount = 0;
        HashMap<String , Integer> map = new HashMap();
        for(VehicalData v : todoItems){
           String deviceId =  v.getDeviceId();
           if(map.get(deviceId)!=null){
             map.put(deviceId,map.get(deviceId) + Integer.valueOf(v.getCount()));
           }
           else{
              if(v.getCount()!=null && deviceId !=null)
             map.put(deviceId, Integer.valueOf(v.getCount()));
           }
           
            
            
        }
        
        Iterator hmIterator = map.entrySet().iterator();

        HashMap<String,Integer> returnMap = new HashMap();

        while (hmIterator.hasNext()) {
            Map.Entry mapElement
                = (Map.Entry)hmIterator.next();
            Integer  count = (Integer)mapElement.getValue();
            System.out.println("the value of count is ::" +mapElement.getValue());
            if(count.intValue() >50){
                returnMap.put((String)mapElement.getKey(),120);

            }
            else if(count.intValue() <50 && count.intValue()> 30){
                returnMap.put((String)mapElement.getKey(),60);

            }
            else if(count.intValue() <30){
                returnMap.put((String)mapElement.getKey(),30);

            }
            else{
                returnMap.put((String)mapElement.getKey(),45);
            }
 
         
        }
        

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            String json = objectMapper.writeValueAsString(returnMap);
            System.out.println("output json is ::::  "  +json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


    }


     private CosmosContainer getContainerCreateResourcesIfNotExist(CosmosClient cosmosClient) {

        try {

            if (cosmosDatabase == null) {
                CosmosDatabaseResponse cosmosDatabaseResponse = cosmosClient.createDatabaseIfNotExists(databaseName);
                cosmosDatabase = cosmosClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());
            }

        } catch (CosmosException e) {
            // TODO: Something has gone terribly wrong - the app wasn't
            // able to query or create the collection.
            // Verify your connection, endpoint, and key.
            System.out.println("Something has gone terribly wrong - " +
                "the app wasn't able to create the Database.\n");
            e.printStackTrace();
        }

        try {

            if (cosmosContainer == null) {
                CosmosContainerProperties properties = new CosmosContainerProperties(containerName, "/id");
                CosmosContainerResponse cosmosContainerResponse = cosmosDatabase.createContainerIfNotExists(properties);
                cosmosContainer = cosmosDatabase.getContainer(cosmosContainerResponse.getProperties().getId());
            }

        } catch (CosmosException e) {
            // TODO: Something has gone terribly wrong - the app wasn't
            // able to query or create the collection.
            // Verify your connection, endpoint, and key.
            System.out.println("Something has gone terribly wrong - " +
                "the app wasn't able to create the Container.\n");
            e.printStackTrace();
        }

        return cosmosContainer;
    }
}
