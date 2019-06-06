<a id="getting-started-with-docker-image"></a>
# Getting Started with Docker Image

The following instructions outline how to build a Docker image if you have the binaries of SnappyData.</br>
SnappyData does not provide a Docker image.

Before building the Docker image, ensure that you have already installed and configured the Docker properly. Refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

## Verify that Docker is Installed</br> 
In the command prompt run the command:

```pre
$ docker run hello-world

```

<a id="build-your-docker"></a>
## Build your own Docker image of SnappyData</br>

A sample Dockerfile is provided which you can use to create your own Docker image of SnappyData.

Download the [Dockerfile](https://github.com/SnappyDataInc/snappy-cloud-tools/blob/master/docker/Dockerfile) and
[start](https://github.com/SnappyDataInc/snappy-cloud-tools/blob/master/docker/start) script and place them into a single directory. This Dockerfile uses the SnappyData 1.1.0 build.

Move to that directory and run the following commands with appropriate details:

    $ docker build -t <your-docker-repo-name>/snappydata:<image-tag> -f Dockerfile .
    $ docker push <your-docker-repo-name>/snappydata:<image-tag>

    For example:

    $ docker build -t snappydatainc/snappydata:1.1.0 -f Dockerfile .
    $ docker push snappydatainc/snappydata:1.1.0


## Launch SnappyData</br>
In the command prompt, type the following command to launch the SnappyData cluster in single container.
This fetches the Docker image from your Docker registry, if the image is not available locally, launches the cluster in a container and leads to the Spark shell.

!!! Note
	Ensure that the Docker containers have access to at least 4 GB of RAM on your machine.

```pre
$  docker run -it -p 5050:5050 <your-docker-registry>/snappydata bin/spark-shell
```

The latest Docker image file starts downloading to your local machine. Depending on your network connection, this may take some time. </br>

After you have launched the Spark shell, in the `$ scala>` prompt, follow the steps explained [here](using_spark_scala_apis.md).</br>

For more details about how you can work with the Docker image see [Snappy Cloud Tools](https://github.com/SnappyDataInc/snappy-cloud-tools/tree/master/docker).
