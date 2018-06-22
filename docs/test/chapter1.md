#What Is AWS Batch?

AWS Batch enables you to run batch computing workloads on the AWS Cloud. Batch computing is a common way for developers, scientists, and engineers to access large amounts of compute resources, and AWS Batch removes the undifferentiated heavy lifting of configuring and managing the required infrastructure. AWS Batch is similar to traditional batch computing software. This service can efficiently provision resources in response to jobs submitted in order to eliminate capacity constraints, reduce compute costs, and deliver results quickly.

As a fully managed service, AWS Batch enables developers, scientists, and engineers to run batch computing workloads of any scale. AWS Batch automatically provisions compute resources and optimizes the workload distribution based on the quantity and scale of the workloads. With AWS Batch, there is no need to install or manage batch computing software, which allows you to focus on analyzing results and solving problems. AWS Batch reduces operational complexities, saves time, and reduces costs, which makes it easy for developers, scientists, and engineers to run their batch jobs in the AWS Cloud. 

##Components of AWS Batch

AWS Batch is a regional service that simplifies running batch jobs across multiple Availability Zones within a region. You can create AWS Batch compute environments within a new or existing VPC. After a compute environment is up and associated with a job queue, you can define job definitions that specify which Docker container images to run your jobs. Container images are stored in and pulled from container registries, which may exist within or outside of your AWS infrastructure.
Jobs

A unit of work (such as a shell script, a Linux executable, or a Docker container image) that you submit to AWS Batch. It has a name, and runs as a containerized application on an Amazon EC2 instance in your compute environment, using parameters that you specify in a job definition. Jobs can reference other jobs by name or by ID, and can be dependent on the successful completion of other jobs. For more information, see Jobs.
Job Definitions

A job definition specifies how jobs are to be run; you can think of it as a blueprint for the resources in your job. You can supply your job with an IAM role to provide programmatic access to other AWS resources, and you specify both memory and CPU requirements. The job definition can also control container properties, environment variables, and mount points for persistent storage. Many of the specifications in a job definition can be overridden by specifying new values when submitting individual Jobs. For more information, see Job Definitions

##Submitting a Job

After you have registered a job definition, you can submit it as a job to an AWS Batch job queue. Many of the parameters that are specified in the job definition can be overridden at run time.

To submit a job

1. Open the [AWS Batch console](https://console.aws.amazon.com/batch/).
2. From the navigation bar, select the region to use.
3. In the navigation pane, choose Jobs, Submit job. 
4. Enter the following details:
5. (Array jobs only) Select Run children sequentially to create a SEQUENTIAL dependency for the current array job. This ensures that each child index job waits for its earlier sibling to finish. For example, JobA:1 depends on JobA:0 and so on.
	!!!Note: You can use parameter substitution default values and placeholders in your command. For more information, see Parameters.

6. (Optional) You can specify environment variables to pass to your job's container. This parameter maps to Env in the Create a container section of the Docker Remote API and the --env option to docker run.

	!!!Important:	We do not recommend using plaintext environment variables for sensitive information, such as credential data.

    a. For Key, specify the key for your environment variable.
    
    !!!Note: Environment variables must not start with AWS_BATCH; this naming convention is reserved for variables that are set by the AWS Batch service.

    b. For Value, specify the value for your environment variable.





