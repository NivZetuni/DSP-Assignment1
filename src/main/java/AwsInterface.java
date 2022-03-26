import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

public class AwsInterface {
    private final Ec2Client ec2;
    private final S3Client s3;
    private final SqsClient sqs;

    private boolean terminate;

    public static final String ami = "ami-00e95a9222311e8ed";

    private static final AwsInterface instance = new AwsInterface();

    private AwsInterface(){
        ec2 = Ec2Client.create();
        s3 = S3Client.builder().region(Region.US_EAST_1).build();
        sqs = SqsClient.create();
        terminate = false;
    }

    public AwsInterface(Ec2Client ec2, S3Client s3, SqsClient sqs) {
        this.ec2 = ec2;
        this.s3 = s3;
        this.sqs = sqs;
    }

    public static AwsInterface getInstance()
    {
        return instance;
    }

    public void setTerminate(){
        this.terminate = true;
    }


    //*******EC2*******
    public String createManager() {

        String userData = "#!/bin/bash" + "\n"
                + "aws s3 cp s3://dsp221ass1nivandgil/manager.jar manager.jar" + "\n"
                + "java -jar manager.jar";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .keyName("testtt")
                .userData(Base64.getEncoder().encodeToString(
                        (userData).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("name")
                .value("manager")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "created worker Instance with the ID: %s \n",
                    instanceId);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public List<String> createWorkers(List<String> workers, int howMuchWorkers) {

        String userData = "#!/bin/bash" + "\n"
                + "aws s3 cp s3://dsp221ass1nivandgil/worker.jar worker.jar" + "\n"
                + "java -jar worker.jar";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(howMuchWorkers)
                .minCount(howMuchWorkers)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
//                .keyName("testtt")
                .userData(Base64.getEncoder().encodeToString(
                        (userData).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        Tag tag = Tag.builder()
                .key("name")
                .value("worker")
                .build();

        System.out.println("Created " + howMuchWorkers +" workers.");

        for (Instance instance : response.instances()) {
            String instanceId = instance.instanceId();
            workers.add(instanceId);

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "created worker Instance with the ID: %s \n",
                        instanceId);
            } catch (Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
        return workers;
    }

    public String getManagerID(){
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations())
                    for (Instance instance : reservation.instances())
                        for (Tag tag : instance.tags())
                            if(tag.value().equals("manager") &&
                                    (instance.state().name().toString().equals("pending") || instance.state().name().toString().equals("running")))
                                return instance.instanceId();

                nextToken = response.nextToken();
            } while (nextToken != null);

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return ("");
    }

    public void terminateInstance(String instanceId){
        TerminateInstancesRequest terminateRequest = TerminateInstancesRequest
                .builder()
                .instanceIds(instanceId)
                .build();

        ec2.terminateInstances(terminateRequest);

        System.out.println("Terminated instance with id: " +instanceId);
    }


    //*******S3*******
    public S3Client getS3() {
        return s3;
    }

    public void createBucket(String bucketName) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration
                                .builder()
                                .build())
                .build());

        System.out.println("successfully created bucket: " + bucketName);
    }

    public void putObjectInBucket(String bucketName,String inputPath, String fileName){
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .acl(String.valueOf(BucketCannedACL.PUBLIC_READ))
                .build();


        s3.putObject(objectRequest, Paths.get(inputPath));

        System.out.println("Uploaded the input files to S3 to bucket " +bucketName);
    }

    public void getObjectInBucket(File file, String bucketName, String key){
        this.s3.getObject(GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key).build(),
                ResponseTransformer.toFile(file));
    }


    //*******SQS*******
    public String createMsgQueue(String queueName){
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    public void sendMessage(String queueUrl, String msg) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(msg)
                .build();
        sqs.sendMessage(sendMessageRequest);
    }

    public List<Message> getMessage(String queueUrl) {
        List<Message> messages=null;
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .visibilityTimeout(100)
                    .build();

            messages = sqs.receiveMessage(receiveMessageRequest).messages();

            while (messages.size() == 0 && !this.terminate) {
                messages = sqs.receiveMessage(receiveMessageRequest).messages();
            }

            if (!this.terminate){
                System.out.println("Received a message:");
                System.out.println(messages.get(0).body());
            }

        }
        catch (SqsException e) {
            System.err.println("somthing went wrong: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return messages;
    }

    public List<Message> getMessageLocal(String queueUrl) {
        List<Message> messages=null;
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .build();

            messages = sqs.receiveMessage(receiveMessageRequest).messages();

            // Busy wait for new message
            while (messages.size() == 0) {
                messages = sqs.receiveMessage(receiveMessageRequest).messages();
            }

            System.out.println("Received a message:");
            System.out.println(messages.get(0).body());


        }
        catch (SqsException e) {
            System.err.println("somthing went wrong: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return messages;
    }

    public String findQueue(String queueName) {
        String urlOfQueue = "";
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder()
                    .queueNamePrefix(queueName)
                    .build();

            ListQueuesResponse listQueuesResponse;
            do{
                listQueuesResponse = sqs.listQueues(listQueuesRequest);
            }
            while(listQueuesResponse.queueUrls().size() == 0);

            urlOfQueue = listQueuesResponse.queueUrls().get(0);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return urlOfQueue;
    }

    public void deleteMsgQueue ( String queueUrl, Message msg ){
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
    }

    public void deleteQueue(String queueName){
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteQueueRequest);
            System.out.println("Succesfully delete the queue with the name:" + queueName);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

}
