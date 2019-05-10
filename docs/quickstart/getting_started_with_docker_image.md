<a id="getting-started-with-docker-image"></a>
# Getting Started with Docker Image

Following steps outline how to build a Docker image if one has the binaries of TIBCO ComputeDB.
TIBCO ComputeDB does not provide a Docker image.

Before building the Docker image, ensure you have already installed and configured Docker properly. Refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

## Verify that Docker is Installed</br> 
In the command prompt run the command:

```pre
$ docker run hello-world

```

<a id="build-your-docker"></a>
## Build your own Docker image of TIBCO ComputeDB</br>

We have provided a sample Dockerfile which you can use to create your own Docker image of TIBCO ComputeDB.
Download the [Dockerfile]() and [start]() script place into a single directory. This Dockerfile uses the SnappyData 1.1.0 build.
Move to that directory and run below commands with appropriate details.

    $ docker build -t <your-docker-repo-name>/snappydata:<image-tag> -f Dockerfile .
    $ docker push <your-docker-repo-name>/snappydata:<image-tag>

    For example:

    $ docker build -t snappydatainc/snappydata:1.1.0 -f Dockerfile .
    $ docker push snappydatainc/snappydata:1.1.0


## Launch TIBCO ComputeDB</br>
In the command prompt, type the following command to launch the TIBCO ComputeDB cluster in single container.
This fetches the Docker image from your Docker registry, if the image is not available locally, and launches the cluster in a container and takes you to the Spark shell.

!!! Note
	Ensure that the Docker containers have access to at least 4GB of RAM on your machine.

```pre
$  docker run -it -p 5050:5050 <your-docker-registry>/snappydata bin/spark-shell
```

The latest Docker image file starts downloading to your local machine. Depending on your network connection, it may take some time. </br>

Once you have launched the Spark shell, in the `$ scala>` prompt, follow the steps explained [here](using_spark_scala_apis.md).</br>

For more details about how you can work with the Docker image see [Snappy Cloud Tools](https://github.com/SnappyDataInc/snappy-cloud-tools/tree/master/docker).
