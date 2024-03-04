package com.lgypro;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetQueueAttributes {
    public static void main(String[] args) {
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.AP_NORTHEAST_2)
                .build();
        String queueName = "cloudtrail-bucket-event-store";
        try (sqsClient) {
            getAttributes(sqsClient, queueName);
        }
    }

    public static void getAttributes(SqsClient sqsClient, String queueName) {
        try {
            GetQueueUrlResponse getQueueUrlResponse = sqsClient
                    .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            // Specify the attributes to retrieve.
            List<QueueAttributeName> atts = new ArrayList<>();
            atts.add(QueueAttributeName.ALL);
            GetQueueAttributesRequest attributesRequest = GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(atts)
                    .build();
            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(attributesRequest);
            Map<String, String> queueAtts = response.attributesAsStrings();
            for (Map.Entry<String, String> queueAtt : queueAtts.entrySet())
                if (queueAtt.getKey().equals("Policy")) {
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    JsonElement element = JsonParser.parseString(queueAtt.getValue());
                    System.out.println(gson.toJson(element));
                } else {
                    System.out.println("Key = " + queueAtt.getKey() +
                            ", Value = " + queueAtt.getValue());
                }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
