import software.amazon.awssdk.services.sqs.model.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Manager {

    final static AwsInterface awsInterface = AwsInterface.getInstance();

    private String toManager;
    private List<String> workers;
    private String toWorker;
    private String workerToManager;
    private ConcurrentHashMap <String, NewTask> tasks;
    private boolean shouldTerminate;
    private String terminateLocalApp;
    private boolean doneTerminate;
    private int counter;

    public static void main(String[] args){
        Manager manager = new Manager();
        System.out.println(manager.getAndSetCounter()+") manager was created");
        manager.executeManager();
    }

    public Manager(){
        this.counter =1;

        System.out.println(getAndSetCounter()+ ") Creating new manager");

        this.toManager= awsInterface.createMsgQueue("toManager");
        System.out.println(getAndSetCounter()+") Created to manager queue with the url:" + toManager);

        this.workers = new ArrayList<>();

        this.toWorker = awsInterface.createMsgQueue("toWorker");
        System.out.println(getAndSetCounter()+") Created to worker queue with the url:" + toWorker);

        this.workerToManager = awsInterface.createMsgQueue("workerToManager");
        System.out.println(getAndSetCounter()+") Created workerToManager queue with the url:" + workerToManager);

        this.shouldTerminate = false;
        this.terminateLocalApp = "";
        this.tasks = new ConcurrentHashMap<>();
        this.doneTerminate = false;
    }

    public void executeManager(){

        Thread localsThread = new Thread(() -> {
            try {

                while (!shouldTerminate) {

                    System.out.println(getAndSetCounter()+")Listening to localApps");
                    List<Message> messages = awsInterface.getMessage(toManager);

                    //check if sqs is on terminate mode
                    if (messages.isEmpty()){
                        break;
                    }
                    System.out.println(getAndSetCounter()+") got message!");
                    awsInterface.deleteMsgQueue(toManager, messages.get(0));

                    handleLocalAppMessage(messages);
                }
            }catch (SqsException e) {
                System.err.println("somthing went wrong: " + e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        });

        Thread workersThread = new Thread(() -> {
            System.out.println(getAndSetCounter()+")Listening to workers");
            while (true) {
                System.out.println("waiting for message from worker");
                try {
                    List<Message> messages = awsInterface.getMessage(workerToManager);
                    //check if sqs is on terminate mode
                    if (messages.isEmpty()){
                        break;
                    }
                    System.out.println(getAndSetCounter()+") got message!");
                    awsInterface.deleteMsgQueue(workerToManager, messages.get(0));

                    handleWorkersMessage(messages);
                    System.out.println(getAndSetCounter()+")done handle message from worker");

//                    check if need to terminate
                    if(doTerminateIfNeeded()){
                        break;
                    }

            }catch (SqsException e) {
                System.err.println("somthing went wrong: " + e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }});

        ExecutorService pool = Executors.newFixedThreadPool(7);
        pool.execute(localsThread);
        pool.execute(localsThread);
        pool.execute(localsThread);
        pool.execute(workersThread);
        pool.execute(workersThread);
        pool.execute(workersThread);
        pool.execute(workersThread);

        pool.shutdown();
    }

    public int getAndSetCounter(){
        int oldCounter = this.counter;
        this.counter = this.counter+1;
        return oldCounter;
    }


//    local thread methods *************

    private void handleLocalAppMessage(List<Message> messages) {
            if (!shouldTerminate) {
                for (Message msg : messages) {
                    System.out.println(getAndSetCounter()+")the msg:\n" + msg.body());
                    String[] splitMsg = msg.body().split(" ");
                    String messageType = splitMsg[0];
//                    check if the message is a terminate message:
                    if (messageType.equals("terminate")) {
                        System.out.println("*********** Got terminate message ***********");
                        this.shouldTerminate = true;
                        this.terminateLocalApp = splitMsg[1];
                        awsInterface.sendMessage(workerToManager,"terminate");
                    } else {
                        String input_url = splitMsg[1];
                        String localAppQueue = splitMsg[2];
                        String bucketName = splitMsg[3];
                        int n = Integer.parseInt(splitMsg[4]);

//                        creating new task from the localApp and put it in the tasks hashMap:
                        NewTask newTask = new NewTask(bucketName, input_url, localAppQueue, n);
                        tasks.put(bucketName, newTask);


                        System.out.println(getAndSetCounter()+") check what the value of numofjobs: " + newTask.getNumOfJobs());

//                        parse the task for jobs and send it to the workers:
                        parseTaskAndSendToWorkers(newTask);
                        System.out.println(getAndSetCounter()+") done local thread part: parse the task and send it to workers queue");
                    }
                }
            }
    }

    private void parseTaskAndSendToWorkers(NewTask newTask) {
        System.out.println(getAndSetCounter()+") start to parsing message");
        try {
//            getting the file from the bucket and parse it to lines:
            File file = new File("input "+newTask.getBucketName());
            System.out.println(getAndSetCounter()+") get the file from s3");
            awsInterface.getObjectInBucket(file,newTask.getBucketName(),"input");

            System.out.println(getAndSetCounter()+") splitting the file into lines");
            byte[] encoded  = Files.readAllBytes(file.toPath());
            String stringInput = new String(encoded,StandardCharsets.US_ASCII);
            String[] inputLines = stringInput.split("\n");
            newTask.setNumOfJobs(inputLines.length);

//            creating workers if needed (check it with the workerRatio):
            int numOfWorkersForTask;
            if((newTask.getNumOfJobs() % newTask.getN()) == 0){
                numOfWorkersForTask = (newTask.getNumOfJobs()/ newTask.getN());
            }
            else{
                numOfWorkersForTask = (newTask.getNumOfJobs()/ newTask.getN())+1;
            }
            System.out.println(getAndSetCounter()+")number of workers to the task: "+ numOfWorkersForTask);
            System.out.println(getAndSetCounter()+")workers size is: "+ workers.size());

            if(numOfWorkersForTask > workers.size()){
                System.out.println(getAndSetCounter()+"workers need to create: "+ (numOfWorkersForTask - workers.size()));
                createNewWorkers(numOfWorkersForTask - workers.size());
            }

//            send the jobs to the workers
            System.out.println(getAndSetCounter()+") send the jobs to the workers...");
            int numOfJob= 0;
            for (String line : inputLines) {
                System.out.println("\t" + line);
                String[] splattedLine = line.split("\t");
                String workerTask = splattedLine[0] + " " + splattedLine[1].substring(0,splattedLine[1].length()) + " " + newTask.getBucketName()+ " " + numOfJob;
                System.out.println(getAndSetCounter()+") sending msg to the workers with the body:\n" + workerTask);
                newTask.getSummaryArrayList().add(new Summary(numOfJob));
                numOfJob++;

                awsInterface.sendMessage(toWorker,workerTask);
            }

            file.delete();
            System.out.println(getAndSetCounter()+") Done! number of jobs that send to workers is: " + numOfJob);
        } catch (Exception e) {
            System.err.println("something went wrong: "+ e.getMessage());
        }
    }

    private synchronized void createNewWorkers(int workerToCreate) {
        if(workers.size() < 19) { //check that we not hit the max number of workers in aws
            int howMuchWorkers;
            //if the amount of workers after adding the workers is over 19, then we add the max number of workers.
            if (workers.size() + workerToCreate > 19) {
                howMuchWorkers = 19 - workers.size();
            }
            else {
                howMuchWorkers = workerToCreate;
            }
            System.out.println(getAndSetCounter()+"workers that we actually add: "+ howMuchWorkers);
            this.workers = (awsInterface.createWorkers(workers, howMuchWorkers));
        }
        else{
            System.out.println(getAndSetCounter()+"num of workers has reached their maximum size according to aws");
        }
    }



//    worker thread methods *************

    private void handleWorkersMessage(List<Message> messages){
        try{
            for (Message msg : messages) {
                String[] splitMsg = msg.body().split("\t");
                System.out.println(getAndSetCounter()+")the message:\n" + messages.get(0).body());
                String src_url = splitMsg[0];
//                check if the message is a terminate message from the local
                if (src_url.equals("terminate")) {
                    System.out.println(getAndSetCounter()+")got terminate message");
                }
//                of not a terminate message, parse the message to failed message and succeeded message
                else if (src_url.equals("Error")) {
                    System.out.println(getAndSetCounter()+")operation faild");
                    String src2_url = splitMsg[1];
                    String bucketName = splitMsg[2];
                    String op = splitMsg[3];
                    String desc = splitMsg[5];
                    int index = Integer.parseInt(splitMsg[4]);
                    System.out.println(bucketName);
                    NewTask task = tasks.get(bucketName);
//                    check if the task is still exist in the manager
                    if (task != null) {
                        if (task.getSummaryArrayList().get(index).isUpdate()) {
                            task.getSummaryArrayList().get(index).setFailSummary(op, src2_url, desc);
                            doneJobHandler(bucketName);
                        }
                    }

                } else {
                    System.out.println(getAndSetCounter()+")operation succeed");
                    String trg_url = splitMsg[1];
                    String operation = splitMsg[2];
                    String bucketName = splitMsg[3];
                    int index = Integer.parseInt(splitMsg[4]);
                    System.out.println(bucketName);
                    NewTask task = tasks.get(bucketName);
//                    check if the task is still exist in the manager
                    if (task != null) {
                        if (task.getSummaryArrayList().get(index).isUpdate()) {
                            task.getSummaryArrayList().get(index).setSuccSummary(src_url, trg_url, operation);
                            doneJobHandler(bucketName);
                        }
                    }
                }

            }
        }catch (SqsException e) {
            System.err.println("somthing went wrong: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }


    }

    private synchronized void doneJobHandler(String bucketName) {
        tasks.get(bucketName).incNumOfDoneJobs();
        System.out.println(getAndSetCounter()+")complete "+tasks.get(bucketName).getNumOfDoneJobs() +"\t" +tasks.get(bucketName).getNumOfJobs()  );
//        check if the task is done
        if (tasks.get(bucketName).getNumOfJobs() == tasks.get(bucketName).getNumOfDoneJobs()) {
            System.out.println(getAndSetCounter()+")Task is Done!");
            summarizeAndSendToLocal(bucketName, tasks.get(bucketName).getSummaryArrayList());
        }
    }

    private void summarizeAndSendToLocal(String bucketName, ArrayList<Summary> summaryArrayList) {
        int counterFail = 0;
        int counterSucc = 0;
        try {
            String fileName = "summary_"+bucketName+".txt";
            FileWriter file = new FileWriter(fileName);
            for(Summary summary : summaryArrayList){
                if(summary.getStatusOfJob().equals("fail")){
                    file.write( summary.getOp() +"\t" + summary.getSrcUrl() +"\t" + "" + summary.getDesc() + "\t" + "fail" +  "\n");
                    counterFail++;
                }
                else{
                    counterSucc++;
                    file.write(""+summary.getOp()+"\t" + summary.getSrcUrl() +"\t" + summary.getTrgUrl() + "\n");
                }
            }
            file.close();
            File file2 = new File(fileName);
            awsInterface.putObjectInBucket(bucketName,"./"+fileName, fileName);
            awsInterface.sendMessage(tasks.get(bucketName).getLocalAppQueue(),"missionComplete");
            file2.delete();
            System.out.println(getAndSetCounter()+")fail: "+counterFail + "\t" + "success: " + counterSucc);
            System.out.println(getAndSetCounter()+")Successfully wrote to the file.");


        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        tasks.remove(bucketName);
    }

    private synchronized boolean doTerminateIfNeeded() {
        if (tasks.isEmpty() && shouldTerminate && !doneTerminate) {
            System.out.println(getAndSetCounter()+")******* no more tasks to do, starting terminate *******");
            try {
                for(String worker : workers){
                    System.out.println(getAndSetCounter()+")send terminate message to workers");
                    awsInterface.sendMessage(toWorker,"terminate");
                }
                awsInterface.setTerminate();
                System.out.println(getAndSetCounter()+")going to sleep..");
                Thread.sleep(1000);
                System.out.println(getAndSetCounter()+")wake up!");
                //***delete queues:
                awsInterface.deleteQueue("workerToManager");
                awsInterface.deleteQueue("toManager");
                awsInterface.deleteQueue("toWorker");

                //***delete workers instances:
                for (String worker : workers) {
                    awsInterface.terminateInstance(worker);
                    System.out.println(getAndSetCounter()+")******* terminate worker with id: " + worker + "*******");
                }
                //***send message to the localapp that terminate to terminate:

                System.out.println(getAndSetCounter()+")******* sending terminate message to local app *******");
                awsInterface.sendMessage(terminateLocalApp, "shouldTerminate");
                doneTerminate = true;
            } catch (InterruptedException e) {
                System.err.println("somthing went wrong: "+ e.getMessage());
            }
            return true;
        }
        return false;
    }


}
