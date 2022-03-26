import java.util.concurrent.atomic.AtomicBoolean;

public class Summary {
    private String srcUrl;
    private String trgUrl;
    private String op;
    private int index;
    private String statusOfJob;
    private String desc;
    private AtomicBoolean isUpdate;


    public Summary(String srcUrl, String trgUrl, String op, int index){
        this.srcUrl = srcUrl;
        this.trgUrl = trgUrl;
        this.op = op;
        this.index = index;
        this.isUpdate = new AtomicBoolean();

    }

    public Summary(int index){
        this.srcUrl = "";
        this.trgUrl = "";
        this.op = "";
        this.statusOfJob ="";
        this.desc="";
        this.index = index;
        this.isUpdate = new AtomicBoolean();;

    }

    public void setSuccSummary(String srcUrl, String trgUrl, String op){
//        this.isUpdate = true;
        this.statusOfJob = "success";
        this.srcUrl = srcUrl;
        this.trgUrl = trgUrl;
        this.op = op;

    }

    public void setFailSummary(String op, String srcUrl, String desc){
//        this.isUpdate = true;
        this.statusOfJob = "fail";
        this.srcUrl = srcUrl;
        this.op = op;
        this.desc = desc;

    }

    public String getSrcUrl() {
        return srcUrl;
    }
    public String getTrgUrl() {
        return trgUrl;
    }
    public String getOp() {
        return op;
    }
    public int getIndex() {
        return index;
    }
    public boolean isUpdate(){
        return isUpdate.compareAndSet(false, true);
    }
    public String getStatusOfJob() {
        return statusOfJob;
    }

    public String getDesc() {
        return desc;
    }
}
