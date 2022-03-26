import org.apache.pdfbox.io.*;
import org.apache.pdfbox.rendering.*;
import org.apache.pdfbox.text.*;
import software.amazon.awssdk.core.sync.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.model.*;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.tools.PDFText2HTML;
import javax.imageio.*;
import java.awt.image.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public class Worker {
    final static AwsInterface awsInterface = AwsInterface.getInstance();
    private String toWorker;
    private String mangerQueue;
    private String bucketName;
    private int counter;


    public static void main(String[] args){
        Worker worker = new Worker();
        System.out.println(worker.getAndSetCounter()+") manager was created");
        worker.executeWorker();
    }

    public Worker(){
        this.counter=0;
        System.out.println(getAndSetCounter()+") Creating new worker.");
        System.out.println(getAndSetCounter()+") searching toWorker queue..");
        this.toWorker = awsInterface.findQueue("toWorker");
        System.out.println(getAndSetCounter()+") found queue with url: " + toWorker);
        System.out.println(getAndSetCounter()+") searching workerToManager queue..");
        this.mangerQueue = awsInterface.findQueue("workerToManager");
        System.out.println(getAndSetCounter()+") found queue with url: " + mangerQueue);
        bucketName = "";
    }

    public int getAndSetCounter(){
        int oldCounter = this.counter;
        this.counter = this.counter+1;
        return oldCounter;
    }

    private void executeWorker(){
        while (true) {
            System.out.println(getAndSetCounter()+") waiting for message in  to worker queue..");
            List<Message> messages = awsInterface.getMessage(toWorker);

            Message msg = messages.get(0);

            if (msg.body().equals("terminate")){
                break;
            }


            //read message
            String[] parts = msg.body().split(" ");
            String op = parts[0];
            String urlStr = parts[1];
            this.bucketName = parts[2];
            String fileNum = parts[3];

            try {
                System.out.println(getAndSetCounter()+") try to work on message");
                System.out.println(getAndSetCounter()+") get the PDF from the URL..");
                File pdf = getPdfFile(urlStr);
                System.out.println(getAndSetCounter()+") got PDF! now try to work on the PDF..");
                File output = work(op, pdf);
                System.out.println(getAndSetCounter()+") work done! store the outputfile in s3");
                storeOutput(output);
                String outputUrl = "https://" + bucketName + ".s3.amazonaws.com/" + output.getName();
                String msgBack = urlStr + "\t" + outputUrl + "\t" + op + "\t" + bucketName + "\t" + fileNum;
                System.out.println(getAndSetCounter()+") sending msg to manager with the SUCC msg:\n" + msgBack);
                awsInterface.sendMessage(mangerQueue,msgBack);

            } catch (IOException e) {// tell manager something when wrong.
                String errorValue = "Error: " + e.getClass().getSimpleName() + ": " + e.getMessage();
                String errorMsg = "Error" + "\t" + urlStr + "\t" + bucketName + "\t" + op +"\t" + fileNum +"\t" + errorValue;
                System.out.println(getAndSetCounter()+") sending msg to manager with the ERR msg:\n" +errorMsg);
                awsInterface.sendMessage(mangerQueue,errorMsg);
            }
            awsInterface.deleteMsgQueue(toWorker,msg);

        }

    }



    private void storeOutput(File output) {
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(output.getName())
                .acl(String.valueOf(BucketCannedACL.PUBLIC_READ))
                .build();

        awsInterface.getS3().putObject(objectRequest, RequestBody.fromFile(output));
    }


    private File work(String op, File pdf) throws IOException {
        File output;

        if(op.equals("ToImage"))
            output = toImage(pdf);
        else if(op.equals("ToHTML"))
            output = toHtml(pdf);
        else
            output = toText(pdf);

        return output;
    }

    private File getPdfFile(String urlStr) throws IOException {
        File pdf = new File(urlStr.substring(urlStr.lastIndexOf('/') + 1));
        URL url = new URL(urlStr);
        HttpURLConnection httpcon = (HttpURLConnection) url.openConnection();
        httpcon.addRequestProperty("User-Agent", "Mozilla/4.0");
        InputStream in = httpcon.getInputStream();
        Files.copy(in, Paths.get(pdf.getPath(), new String[0]), new java.nio.file.CopyOption[0]);
        return pdf;
    }

    private File toText(File pdf) throws IOException {
//    setting up the new file
        File output = new File(pdf.getName().replace("pdf", "txt"));
        try{
    //    load PDF file
            PDDocument doc = PDDocument.load(pdf);
            //    setting up to take only 1 page:
            PDFTextStripper pdfStripper = new PDFTextStripper();
            pdfStripper.setStartPage(0);
            pdfStripper.setEndPage(1);
            StringWriter writer = new StringWriter();
            pdfStripper.writeText(doc, writer);

            FileWriter fileWriter = new FileWriter(output);
            fileWriter.write(writer.toString());
            fileWriter.flush();
            fileWriter.close();
            doc.close();

        }finally {
            if (pdf.delete()) {
                System.out.println("Deleted the file: " + pdf.getName());
            } else {
                System.out.println("Failed to delete the file.");
            }
        }
            return output;
    }

    private File toHtml(File pdf) throws IOException  {

        String newName = pdf.getName().replace("pdf", "html");
        File output = new File(newName);
        try {
            PDDocument doc = PDDocument.load(pdf, MemoryUsageSetting.setupTempFileOnly());
            PDFText2HTML stripper = new PDFText2HTML();
            stripper.setStartPage(0);
            stripper.setEndPage(1);
            StringWriter writer = new StringWriter();
            stripper.writeText(doc, writer);
            FileWriter fileWriter = new FileWriter(output);
            fileWriter.write(writer.toString());
            fileWriter.flush();
            fileWriter.close();
            doc.close();
        }
        finally {
            if (pdf.delete()) {
                System.out.println("Deleted the file: " + pdf.getName());
            } else {
                System.out.println("Failed to delete the file.");
            }
    }
        return output;
    }

    private File toImage(File pdf) throws IOException {
        try {
            String newName = pdf.getName().replace("pdf", "png");
            File output = new File(newName);

            PDDocument doc = PDDocument.load(pdf);
            PDFRenderer pdfRenderer = new PDFRenderer(doc);
            BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300, ImageType.RGB);
            ImageIO.write(bim, "png", output);
            doc.close();
            return output;
        }
        finally {
            if (pdf.delete()) {
                System.out.println("Deleted the file: " + pdf.getName());
            } else {
                System.out.println("Failed to delete the file.");
            }
        }


    }

}