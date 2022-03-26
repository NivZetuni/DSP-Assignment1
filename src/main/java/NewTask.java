import java.util.ArrayList;

public class NewTask {
    private String bucketName;
    private String inputUrl;
    private String localAppQueue;
    private int n;
    private int numOfJobs;
    private int numOfDoneJobs;
    private ArrayList<Summary> summaryArrayList;

    public NewTask(String bucketName, String inputUrl,String localAppQueue, int n){
        this.bucketName = bucketName;
        this.inputUrl = inputUrl;
        this.localAppQueue = localAppQueue;
        this.n = n;
        this.summaryArrayList = new ArrayList<>();

    }

    public String getBucketName() {
        return bucketName;
    }
    public String getInput_url() {
        return inputUrl;
    }
    public String getLocalAppQueue() {
        return localAppQueue;
    }
    public int getN() {
        return n;
    }
    public int getNumOfJobs() {
        return numOfJobs;
    }
    public ArrayList<Summary> getSummaryArrayList() { return summaryArrayList;}
    public void incNumOfDoneJobs() {
        numOfDoneJobs++;
    }
    public int getNumOfDoneJobs(){
        return numOfDoneJobs;
    }

    public void setNumOfJobs(int numOfLobs){
        this.numOfJobs = numOfLobs;
    }

}
