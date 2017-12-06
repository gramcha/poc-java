package s3Tester.s3App;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class GetObject {
	
	//

	public static void main(String[] args) throws IOException {
		//reading s3 bucket
		s3BucketRead();
		
		//Reading SQS queue...
		//readSQSQueue();
		
		//send message to sqs - for testing
		//sendSQSMessge();
	}

	private static void s3BucketRead() throws IOException {
		//http://docs.aws.amazon.com/AmazonS3/latest/dev/RetrievingObjectUsingJava.html
		//http://docs.ceph.com/docs/master/radosgw/s3/java/
		String bucketName = "sports.csv";
		String accessKey = "##############################";
		String secretKey = "############################";

		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		//AmazonS3 s3Client = new AmazonS3Client(credentials);
		AmazonS3 s3Client = new AmazonS3Client();//http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html this will get credentials from env/ ~/.aws/credentials file.
		
//		List<Bucket> buckets = s3Client.listBuckets();
//		for (Bucket bucket : buckets) {
//		        System.out.println(bucket.getName() + "\t" +
//		                StringUtils.fromDate(bucket.getCreationDate()));
//		}
		try {
			System.out.println("Downloading an object");
			S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, "sample.csv"));
			System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());
			displayTextInputStream(s3object.getObjectContent());

			// Get a range of bytes from an object.

//			GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, "sample.csv");
//			//rangeObjectRequest.setKey(key);
//			rangeObjectRequest.setRange(0, 10);
//			S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

//			System.out.println("Printing bytes retrieved.");
//			displayTextInputStream(objectPortion.getObjectContent());

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which" + " means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means" + " the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void sendSQSMessge(){
		AmazonSQS sqs = new AmazonSQSClient();
		String queueUrl="$$$$$$$$$$$$$$$$$$$$$$$$$$";
		SendMessageResult result = sqs.sendMessage(new SendMessageRequest(queueUrl, "{'message':'sample message for testing'}"));
		System.out.println("send message fired...");
		System.out.println(result.toString());
	}
	static String prityprint(String uglyJSONString){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(uglyJSONString);
		return gson.toJson(je);
	}
	/* // https://forums.aws.amazon.com/message.jspa?messageID=220494
	 * //http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html
	 * sqs limitations
	 * 	- it will not return all the messages in single call. 
	 * 	- it is our responsibility to call multiple times to ensure all the messages are received.
	 * 	- it is our responsibility to delete the message in queue.
	 * 	- order is not maintained.
	 * 	- most of the time request for message will return single message even though asked for max numbers
	 * 	- in these cases we have to send multiple request to read all the messages in queue. 
	 * 	- below example readSQSQueue() will make sure reading all the messages available.
	 * 	- requires polling from our side
	 * sqs adavantage
	 * 	- SQS is distributed
	 * 	- it will copy and reatain the messages across multiple servers in multiple data centers.
	 * 	- our message is safe.
	 * 
	 * */
	private static void readSQSQueue() {
		
		AmazonSQS sqs = new AmazonSQSClient();
		int NullMessageCount=0;
		int NullMessageCountTarget=2;
		//Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        //sqs.setRegion(usWest2);
		while (NullMessageCount < NullMessageCountTarget)
        {
			String queueUrl="$$$$$$$$$$$$$$$$$$$$$$$$$$";
			System.out.println("Receiving messages from MyQueue.\n");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
			receiveMessageRequest.setMaxNumberOfMessages(10);
			
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
			if(messages.size()==0){
				NullMessageCount++;
			}
			System.out.println(messages.size());
			for (Message message : messages) {
			    System.out.println("  Message");
			    System.out.println("    MessageId:     " + message.getMessageId());
			    System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
			    System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
			    System.out.println("    Body:          " + prityprint(message.getBody()));
			    for (Entry<String, String> entry : message.getAttributes().entrySet()) {
			        System.out.println("  Attribute");
			        System.out.println("    Name:  " + entry.getKey());
			        System.out.println("    Value: " + entry.getValue());
			    }
			    if(message.getMD5OfBody().equals("2e79c18c4496cbbd63ce16bdfe53c989"))
			    {
			    	//delete this hash generated by sendSQSMessge() for "{'message':'sample message for testing'}"
			    	deleteMessage(sqs,queueUrl,message.getReceiptHandle());
			    }
			}
			System.out.println();
        }
		
	}
	static void deleteMessage(AmazonSQS sqs, String myQueueUrl, String messageReceiptHandle)
	{
		System.out.println("deleting this message - warning !!!!!"); 
		sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
	}
	private static void displayTextInputStream(InputStream input) throws IOException {
		// Read one text line at a time and display.
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;

			System.out.println("    " + line);
		}
		System.out.println();
	}
}
