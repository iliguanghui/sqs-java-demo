package com.lgypro;

import com.google.gson.JsonParser;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;

public class LongPolling {
    private static final String queueName = "cloudtrail-bucket-event-store";

    public static void main(String[] args) {
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.AP_NORTHEAST_2)
                .build();
        try (sqsClient) {
            receiveMessage(sqsClient);
        }
    }

    public static void receiveMessage(SqsClient sqsClient) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        String queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();
        int numberOfMessage = 10;
        int count = 0;
        while (count < numberOfMessage) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20).build();
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            List<String> receiptHandleList = new ArrayList<>();

            List<Message> messages = receiveMessageResponse.messages();
            count += messages.size();
            messages.forEach(message -> {
                receiptHandleList.add(message.receiptHandle());
                JsonParser.parseString(message.body())
                        .getAsJsonObject()
                        .get("Records")
                        .getAsJsonArray()
                        .asList()
                        .stream().map(element -> element.getAsJsonObject()
                                .get("s3")
                                .getAsJsonObject()
                                .get("object")
                                .getAsJsonObject()
                                .get("key")
                                .getAsString()).forEach(System.out::println);
            });
            List<DeleteMessageBatchRequestEntry> requestEntries = new ArrayList<>();
            for (int i = 0; i < receiptHandleList.size(); i++) {
                requestEntries.add(DeleteMessageBatchRequestEntry.builder()
                        .id(String.valueOf(i))
                        .receiptHandle(receiptHandleList.get(i))
                        .build());
            }
            DeleteMessageBatchRequest batchDeleteRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(requestEntries)
                    .build();
            sqsClient.deleteMessageBatch(batchDeleteRequest);
        }
    }
}
