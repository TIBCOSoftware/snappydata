<a id="getting-started-with-docker-image"></a>
# Getting Started with Docker Image

SnappyData comes with a pre-configured Docker image. It has binaries for SnappyData, which enables you to try the quick start program and more, with SnappyData.

This section assumes you have already installed and configured Docker properly. Refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

## Verify that Docker is Installed</br> 
In the command prompt run the command:

```pre
$ docker run hello-world

```

!!! Note
	Ensure that the Docker containers have access to at least 4GB of RAM on your machine.

## Get the Docker Image</br>
In the command prompt, type the following command to get the Docker image. This starts the container and takes you to the Spark shell.

```pre
$  docker run -it -p 5050:5050 snappydatainc/snappydata bin/spark-shell
```

The latest image files start downloading to your local machine. Depending on your network connection, it may take some time. </br>

Once you have launched the Spark shell, in the `$ scala>` prompt, follow the steps explained [here](using_spark_scala_apis.md).</br>

For more details about SnappyData Docker image see [Snappy Cloud Tools](https://github.com/SnappyDataInc/snappy-cloud-tools/tree/master/docker).
