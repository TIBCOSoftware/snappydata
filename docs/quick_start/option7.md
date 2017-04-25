<a id="getting-started-with-docker-image"></a>
##Option 7: Getting Started with Docker Image

SnappyData comes with a pre-configured container with Docker. The container has binaries for SnappyData. This enables you to easily try the quick start program and more, with SnappyData.

This section assumes you have already installed Docker and it is configured properly. Refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

**Verify that Docker is installed**: In the command prompt run the command:
```scala
$ docker run hello-world

```

<Note>Note: Ensure that the Docker containers have access to at least 4GB of RAM on your machine</Note>

**Get the Docker Image: ** In the command prompt, type the following command to get the docker image. This starts the container and takes you to the Spark Shell.
```scala
$  docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell
```
It starts downloading the latest image files to your local machine. Depending on your network connection, it may take some time.
Once you are inside the Spark Shell with the "$ scala>" prompt, you can follow the steps explained [here](option1.md#Start_quickStart)

For more details about SnappyData docker image see [Snappy Cloud Tools](https://github.com/SnappyDataInc/snappy-cloud-tools/tree/master/docker)
