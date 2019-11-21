<a id="getting-started-with-docker-image"></a>
# Getting Started with Docker Image

The following instructions outline how to build a Docker image if you have the binaries of SnappyData.</br>

!!!Note
	SnappyData does not provide a Docker image. You must build it explicitly.

Before building the Docker image, ensure the following:

*	You have Docker installed, configured and it runs successfully on your machine. Refer to the [Docker documentation](http://docs.docker.com/installation) for more information on installing Docker.
*	The Docker containers have access to at least 4GB of RAM on your machine.

!!!Note
	To allow non-root users to run Docker commands, follow the instructions [here](https://docs.docker.com/install/linux/linux-postinstall)

## Verifying Docker Installation</br> 
In the command prompt run the command:

```pre
$ docker run hello-world

```

<a id="build-your-docker"></a>
## Building Docker Image of SnappyData</br>

A sample Dockerfile is provided which you can use to create your own Docker image of SnappyData.

Download the [Dockerfile](https://github.com/SnappyDataInc/snappy-cloud-tools/blob/master/docker/Dockerfile) script and place it into a directory. The Dockerfile contains a link to the latest SnappyData OSS version to build the image. 

Move into the directory containing the downloaded Dockerfile and then run the docker build command with the required details to build the docker image. You can create an image using any one of the following options:

*	[Building Image from the Latest Version of SnappyData OSS](#builddockerimagesnappy)
*	[Building Image from URL of a Public Site Containing the Product Tarball](#builddockerurl)
*	[Building Image from Local Copy of Product Tarball](#builddockerimageslocal)


<a id="builddockerimagesnappy"></a>
### Building Image from the Latest Version of SnappyData OSS

```
$ docker build -t <your-docker-repo-name>/<image_name>[:<image-tag>] .
```

!!!Note
	If the `--build-arg` is not provided, it will download the latest version of SnappyData OSS release and build a Docker image for the same.

For example :

```
$ docker build -t myrepo/snappydata . 
```

This builds an image with `latest` tag.

```
$ docker build -t myrepo/snappydata:1.1.1 . 
```

This image will have a tag `1.1.1 ` .

<a id="builddockerurl"></a>
### Building Image from URL of a Public Site Containing the Product Tarball

If you want to create an image from a different version of SnappyData product, you can specify the URL in the command.

```
$ docker build -t myrepo/snappydata . --build-arg URL=<public-url>
```

For example:

```
$ docker build -t myrepo/snappydata . --build-arg URL=https://github.com/SnappyDataInc/snappydata/releases/download/v1.1.1/snappydata-1.1.1-bin.tar.gz
```

<a id="builddockerimageslocal"></a>
### Building Image from Local Copy of Product Tarball

Download SnappyData tarball locally on your machine from [here](https://snappydatainc.github.io/snappydata/install/). Copy the downloaded **tar.gz** file to the docker folder where the Dockerfile is placed. Run the Dockerfile using the following command:

```
$ docker build -t myrepo/snappydata -f Dockerfile . --build-arg URL=<tar-ball name>
```

## Getting Details of Docker Images

After the docker build is successful, you can check the details for docker images using the `docker images` command.

For example:

```
$ docker images

```

## Publishing the Docker Image

If you want to push the image onto Docker hub, login to the Docker account using `docker login` command and provide your docker login credentials, followed by the `docker push` command. For more information, visit [here](https://docs.docker.com/engine/reference/commandline/login).

```
$ docker push <your-docker-repo-name>/<image_name>[:<image-tag>]
```

Ensure to use the same name in the `docker push` that is used in `docker build`.

For example:

```
$ docker push myrepo/snappydata
```
This example only showcases how to push an image onto Docker Hub. You can also publish the image to other container registries such as [gcr.io](http://gcr.io). For gcr.io, you can refer [this document](https://cloud.google.com/container-registry/docs/pushing-and-pulling).

## Launching SnappyData Inside Docker

In the command prompt, execute the following commands to launch the SnappyData cluster in a single container.

```
$ docker run -itd --net=host --name <container-name> <your-docker-repo-name>/<image_name>[:<image-tag>] start all
```

If the image is not available locally, this fetches the Docker image from the Docker registry, launches a basic cluster consisting of one data node, one lead, and one locator in a container.

### For Linux,

```
$ docker run -itd --net=host --name snappydata myrepo/snappydata start all
```

### For Mac OS

!!!Note
	If you are using MAC OS, you must redirect the ports manually. If you use `--net=host`, it may not work correctly on the Mac OS. The following modified command should be used for Mac OS:

```
$ docker run -d --name=snappydata -p 5050:5050 -p 1527:1527 -p 1528:1528 myrepo/snappydata start all -h,ostname-for-clients=<Machine_IP/Public_IP>
```

The `-hostname-for-clients` parameter sets the IP Address or Hostname that the server listens for client connections. The command may take some time to execute.


## Commanly used Docker Commands

| Description| Docker Commands |
|--------|--------|
|      To check details of all the Docker containers  |     `$ docker ps -a `  |
|      To check the Docker Logs  |     `$ docker logs <container-name>`   |
|      To connect SnappyData with the Command Line Client. <br>Use Ctrl+D or type ‘exit;’ to exit the shell.  |     `$ docker exec -it <container-name> ./bin/snappy`   |
|     To launching a Spark shell. <br>Use type ‘:q’ to exit the shell. |     `$ docker exec -it <container-name> bin/spark-shell `  |
|      To stop the Cluster.  |     `$ docker exec -it <container-name> ./sbin/snappy-stop-all.sh `  |
|      To stop the Container.  |     `$ docker stop <container-name> ` |
|      To run commands inside the container. |     `$ docker exec -it snappydata /bin/bash`  |





