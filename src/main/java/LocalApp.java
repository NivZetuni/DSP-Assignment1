import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;

public class LocalApp {

    final static AwsInterface awsInterface = AwsInterface.getInstance();

    private final String inputPath;
    private final String outputPath;
    private final int workersRatio;
    private final boolean shouldTerminate;

    private final String uniqueLocalId;
    private final String bucketName;
    private final String managerToLocalQueue;

    private String toManager;
    private int counter;

    public static void main(String[] args) throws IOException {
        System.out.println("1) starting work!");
        System.out.println("2) check if input is valid");
        boolean shouldTerminate = false;

        if(args.length == 3 || args.length == 4) {
            if (args.length == 4) {
                if (args[3].equals("terminate"))
                    shouldTerminate = true;
                else {
                    System.err.println("Invalid command line argument: " + args[4]);
                    System.exit(1);
                }
            }
        }
        else {
            System.err.println("Invalid number of command line arguments");
            System.exit(1);
        }
        System.out.println("3) input is valid!");
        String inputPath = args[0];
        String outputPath = args[1];
        int workersRatio = Integer.parseInt(args[2]);

        LocalApp newLocalApp = new LocalApp(inputPath, outputPath, workersRatio, shouldTerminate);

        newLocalApp.executeLocalApp();
    }

    public LocalApp (String inputPath, String outputPath, int n, boolean shouldTerminate){
        this.counter = 4;

        this.inputPath = "./"+inputPath;
        this.outputPath = outputPath;
        this.workersRatio = n;
        this.shouldTerminate = shouldTerminate;

        this.uniqueLocalId ="mylocalapp" + new Date().getTime();
        this.bucketName = "mybucket-" +uniqueLocalId;
        this.managerToLocalQueue = awsInterface.createMsgQueue("managerToLocal-"+uniqueLocalId);
        System.out.println( getAndSetCounter()+") created managerToLocal queue with the url: "+ managerToLocalQueue);

        this.toManager = "";
    }

    public void executeLocalApp() {

//        creating a unique bucket and upload the input file to S3:

        System.out.println(getAndSetCounter() + ") creating bucket withe the bucket name: " + bucketName);
        awsInterface.createBucket(bucketName);

        System.out.println(getAndSetCounter() + ") uploding file input to the bucket");
        awsInterface.putObjectInBucket(bucketName, inputPath, "input");


//        get the manager and send to him a message:

        System.out.println(getAndSetCounter() + ") check if manager already exist");
        String managerID = awsInterface.getManagerID();
        if (managerID.equals("")) {
            System.out.println(getAndSetCounter() + ") manager not exist: creating manager... ");
            managerID = awsInterface.createManager();
        }
        System.out.println(getAndSetCounter() + ") manager created! connected to:  " + managerID);
        System.out.println(getAndSetCounter() + ") searching manager queue...");
        this.toManager = awsInterface.findQueue("toManager");
        System.out.println(getAndSetCounter() + ") found manager queue! with the url: " + toManager);

        sendMessageToManager();

        //receiving a message from the manager:

        handleMessageFromManager();

        if (shouldTerminate){
            sendTerminateToManager();
            handleTerminateMessage(managerID);
        }

        awsInterface.deleteQueue("managerToLocal-"+uniqueLocalId);


    }

    private void handleTerminateMessage(String managerID) {
        List<Message> terminateMessage = awsInterface.getMessageLocal(managerToLocalQueue);
        for (Message msg : terminateMessage) {
            if (msg.body().equals("shouldTerminate")) {
                awsInterface.terminateInstance(managerID);
            }
        }
    }

    private void sendTerminateToManager() {
        String info = "terminate"+" " + managerToLocalQueue;
        awsInterface.sendMessage(toManager,info);
    }

    private void handleMessageFromManager() {
            List<Message> messages = awsInterface.getMessageLocal(managerToLocalQueue);
            for (Message msg : messages) {
                if (msg.body().equals("missionComplete")) {
                    convertFileToHtml();
                }
            }

            //***delete bucket
//            awsInterface.deleteBucket(bucketName);
            //***delete queue

    }

    private void convertFileToHtml() {
        try{
            File file = new File("summarize.txt");
            FileWriter outputFile = new FileWriter(outputPath+".html");
            awsInterface.getObjectInBucket(file, bucketName, "summary_"+bucketName+".txt");
//            System.out.println("PASSED IT");
            byte[] encoded  = Files.readAllBytes(file.toPath());
            String stringInput = new String(encoded, StandardCharsets.US_ASCII);
            String[] inputLines = stringInput.split("\n");
            outputFile.write("<div>\n<h2>Summary:</h2><ol>\n");
            for (String line : inputLines){
                String[] splitLine = line.split("\t");
                String op = splitLine[0];
                String srcUrl = splitLine[1];
                String diff = splitLine[2];
                outputFile.write("<li>");
                if(splitLine.length == 4){
                    outputFile.write("<ul style=\"color: red\">");
                }
                else{
                    outputFile.write("<ul>");
                }
                outputFile.write("&lt;" + op + "&gt;:&emsp;&emsp;"+ srcUrl + "&emsp;&emsp;");
                if(splitLine.length == 4){
                    outputFile.write("&lt;"+ diff+"&gt;");
                }
                else{
                    outputFile.write(diff);
                }
                outputFile.write("</ul>\n</li>\n");
            }
            outputFile.write("</ol>\n</div>\n");
            outputFile.close();
            file.delete();
    } catch (IOException e) {
        System.out.println("An error occurred.");
        e.printStackTrace();
    }
    }

    private void sendMessageToManager() {
        String urlOfInput = "https://" + bucketName + ".s3.amazonaws.com/input";

//        maybe the structure of the message should be: <messageType> <uniqueLocalFilePath> <outputFilePath> <workersRatio>
        String info =  "toHandle"+ " " + urlOfInput + " " + managerToLocalQueue + " " + bucketName + " " + workersRatio;

        System.out.println(getAndSetCounter()+") Sending message to manager:\n" + info);
        awsInterface.sendMessage(toManager,info);
        System.out.println(getAndSetCounter()+") message sent!");

    }

    public int getAndSetCounter(){
        int oldCounter = this.counter;
        this.counter = this.counter+1;
        return oldCounter;
    }

}