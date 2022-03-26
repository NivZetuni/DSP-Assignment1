

project information:

	ami: "ami-00e95a9222311e8ed"
	key name: "testtt" (located in the main folder in the .zip file)
	instance type: T2_MICRO
	our run time: input1: 14:40 mins (include creating manager and workers), input2 : 2:20 mins (workers already running).
	n = 400

how to run:

1. update the credentials file.
2. build jar file for each one of the main classes: "manager.jar", "worker.jar" and "localApp.jar".
3. upload the manager and the worker jars to S3, to the bucket : "dsp221ass1nivandgil"
4. put the localApp.jar in the same folder with the input file.
5. run the program through the CMD:
>java -jar localApp.jar <inputFileName> <outputFileName> <n> <[terminate]>



Classes:

NewTask:
	store the needed information for each task. such as: number of jobs (= number of lines), number of jobs done, bucket name, input url, 'n', local app queue and array list of Summary type which contains the summary of each job.

Summary:
	store the information of each done job. status :success / fail ,urls, etc.

AwsInterface:
	includes all the methods to communicate with AWS services (S3, EC2, SQS).


main classes and how the project works:

1. Local app:
	the local app receive the input file name and the n through the CMD. 
	checks if the input file is valid.
	create its own queue named:"managerToLocal" + unique id by the date and time.
	checks if a manger is exist and if not create one, and get its queue (which created by the manger, and its name is "toManger").
	create unique bucket and upload the input file to S3 in the bucket (named: "mybucket-mylocalapp"+ unique id by the date and time).

	created a message of type Message with the information of: "<url of input in S3> <local app unique queue url> <bucket name> <n> <boolean of terminate>" and send it to the manager queue.
	waits for a "missionComplete" message from manager.

	receive a message from the manager  and convert it to HTML.
	if received terminate in the CMD:
		send a terminate message to the manager. 
		wait for termination complete message from the manager and delete the manager instance and the local app queue.


2.Mangarer:
	create the following queues: "toMangaer", "toWorker", "workersToManager".
	create thread pool, which some thread receiving message from local app, and some are waiting for a message from workers.

	threads of the local app:
		
		receiving a messages from the local apps, and delete the message from the queue.

		if "toHandle" message:
			use its data to create a NewTask.
			put the new task in the tasks hashmap (with key of the unique bucket name).
			parse the new task to jobs (which each one is a line in the input file).
			send the jobs to the toWorker queue (with the structure: <operator> <source URL> <bucket name> <line number>).
		if "terminate" message::
			update the shouldTerminate field to true, which makes the threads of the local stop gracefully which resulting no more new tasks from local apps.
			send a message to the workerToManager queue, to update the threads that they should terminate when all task are over.			

	threads of the workers:
		receive a message from the "workerToManager" queue, and delete it.
		parsing the message, checks if the first word is "Error"
		if "Error":
			parse the message to a fail summary (message structure: <operator> <source url> <description>)
			increase the number of done jobs in the NewTask.
		if "Success":
			parse the message to a success summary (message structure: <operator> <source url> <target url>)
			increase the number of done jobs in the NewTask.

		in any case, checks if the number of done jobs equal to the number of total jobs of the task.
		if so, create a summary file, and upload it to the NewTask's bucket.
		send a mission complete message to the local app queue.
		
		if "Terminate":
			continue to work as usual until all task are done (this message is important because if the final task is the terminating task then this will allow 1 worker to start the terminate process).
			after all task are done, one thread send terminate message to the workers instance and make them stop gracefully, delete all queues and allow all the threads in the manager to stop gracefully .

3.Worker:
	find the following queues: "workerToMangaer", "toWorker".

	receive a message from the "toWorker" queue.
	parse the message and get the PDF file from the source url.
	check which operator is needed to be execute, and execute it on the pdf.
	upload the new file to S3.
	send message back to the manager (with the structure: <source url> <target url> <operator> <bucket name> <line number>).
	if there was an error through the process:
	send message back to the manager (with the structure: <"Error"> <source url> <operator> <bucket name> <line number> <Error description>).
	repeat until receive a terminate message, exit the loop and stop gracefully .


Niz Zetuni, 	307852897.
Gil Yadgar, 	311334825.
