<a id="getting-started-with-docker-image"></a>
# Building a Docker Image with SnappyData Binaries

The following instructions outline how to build a Docker image if you have the binaries of SnappyData.</br>

!!!Note
	SnappyData does not provide a Docker image. You must build it explicitly.

Before building the Docker image, ensure the following:

*	You have Docker installed, configured, and it runs successfully on your machine. Refer to the [Docker documentation](http://docs.docker.com/installation) for more information on installing Docker.
*	The Docker containers have access to at least 4GB of RAM on your machine.

!!!Note
	To allow non-root users to run Docker commands, follow the instructions [here](https://docs.docker.com/install/linux/linux-postinstall)

## Verifying Docker Installation</br>
In the command prompt, run the command:

```pre
$ docker run hello-world

```

<a id="build-your-docker"></a>
## Building Docker Image of SnappyData</br>

You can use the Dockerfile that is provided and create your own Docker image of SnappyData. Download the [Dockerfile](https://github.com/SnappyDataInc/snappy-cloud-tools/blob/master/docker/Dockerfile) script and place it into a directory. The Dockerfile contains a link to the latest SnappyData OSS version to build the image.

!!!Note
	To download the Dockerfile on Linux or MAC, use the wget command. </br>` wget https://raw.githubusercontent.com/SnappyDataInc/snappy-cloud-tools/master/docker/Dockerfile`

Move into the directory containing the downloaded Dockerfile and then run the Docker build command with the required details to build the Docker image. You can create an image using any one of the following options:

*	[Building Image from the Latest Version of SnappyData OSS](#builddockerimagesnappy)
*	[Building Image from a URL Directing to SnappyData Binaries](#builddockerurl)
*	[Building Image from Local Copy of SnappyData Product TAR file](#builddockerimageslocal)


<a id="builddockerimagesnappy"></a>
### Building Image from the Latest Version of SnappyData OSS

By default, the Dockerfile creates a Docker image from the latest version of SnappyData OSS.

```
$ docker build -t <your-docker-repo-name>/<image_name>[:<image-tag>] .
```

!!!Note
	If you do not provide any argument to the Dockerfile, the latest version of the SnappyData OSS release is downloaded and a Docker image for the same is built.

For example:

The following command builds an image with tag `latest`:

```
$ docker build -t myrepo/snappydata .
```

The following command builds an image with tag `1.2.0 `:

```
$ docker build -t myrepo/snappydata:1.2.0 .
```

<a id="builddockerurl"></a>
### Building Image from a URL Directing to SnappyData Binaries

If you want to create a Docker image from any of the previous versions of SnappyData, you can specify the URL of the tarfile in the build command.


```
$ docker build -t <your-docker-repo-name>/<image_name>[:<image-tag>] . --build-arg TARFILE_LOC=<public-url>

```

For example:

```
$ docker build -t myrepo/snappydata . --build-arg TARFILE_LOC=https://github.com/SnappyDataInc/snappydata/releases/download/v1.2.0/snappydata-1.2.0-bin.tar.gz
```

<a id="builddockerimageslocal"></a>
### Building Image from Local Copy of SnappyData Product TAR file

If you have already downloaded the SnappyData tarfile locally onto your machine, use the following steps to build an image from the downloaded binaries. To download SnappyData, refer to the [Provisioning SnappyData](https://snappydatainc.github.io/snappydata/install/) section in the product documentation.

Copy the downloaded **tar.gz** file to the Docker folder where you have placed the Dockerfile and run the following command:

```
$ docker build -t <your-docker-repo-name>/<image_name>[:<image-tag>] . --build-arg TARFILE_LOC=<tarfile name>

```

For example:

```
$ docker build -t myrepo/snappydata . --build-arg TARFILE_LOC=snappydata-1.2.0-bin.tar.gz
```


## Verifying Details of Docker Images

After the Docker build is successful, you can check the details for Docker images using the `docker images` command.

For example:

```
$ docker images

```

## Publishing Docker Image

If you want to publish the Docker image onto the Docker Hub, login to the Docker account using `docker login` command, and provide your credentials. For more information on Docker login, visit [here](https://docs.docker.com/engine/reference/commandline/login). After a successful login, you can publish the Docker image using the `docker push` command.

```
$ docker push <your-docker-repo-name>/<image_name>[:<image-tag>]
```
Ensure to use the same name in the `docker push` that is used in `docker build`.

For example:

```
$ docker push myrepo/snappydata
```
!!!Note
	This example only showcases how to push an image onto Docker Hub. You can also publish the image to other container registries such as [gcr.io](http://gcr.io). For publishing on gcr.io, you can refer [this document](https://cloud.google.com/container-registry/docs/pushing-and-pulling).

## Launching SnappyData Container

The command to launch SnappyData container is different for Linux and macOS. 

### Launching SnappyData Container on Linux

In the command prompt, execute the following commands to launch the SnappyData cluster in a single container.

```
$ docker run -itd --net=host --name <container-name> <your-docker-repo-name>/<image_name>[:<image-tag>] start all

# -i: keep the STDIN open even if not attached.
# -t: Allocate pseudo-TTY.
# -d: Detach and run the container in background and print container ID.
# --net=host: Use the Docker host network stack.
```

If the image is not available locally, this fetches the Docker image from the Docker registry, launches a default cluster consisting of one data node, one lead, and one locator in a container.



```
$ docker run -itd --net=host --name snappydata myrepo/snappydata start all

```

### Launching SnappyData Container on macOS

If you are using macOS, you must redirect the ports manually using `-p` parameter. If you use `--net=host`, it may not work correctly on the macOS. You can use the following modified command for macOS:

```
$ docker run -d --name=snappydata -p 5050:5050 -p 1527:1527 -p 1528:1528 myrepo/snappydata start all -hostname-for-clients=<Machine_IP/Public_IP>
```

The `-hostname-for-clients` parameter sets the IP Address or Hostname that the server listens for client connections. The command may take few seconds to execute.


## Commonly used Docker Commands

| Description| Docker Commands |
|--------|--------|
|      To check details of all the Docker containers.  |     `$ docker ps -a `  |
|      To check the container logs.  |     `$ docker logs <container-name>`   |
|      To launch Snappy Shell. |     `$ docker exec -it <container-name> ./bin/snappy`   |
|     To launch Spark Shell.  |     `$ docker exec -it <container-name> ./bin/spark-shell `  |
|      To stop the cluster.  |     `$ docker exec -it <container-name> ./sbin/snappy-stop-all.sh `  |
|      To stop the container.  |     `$ docker stop <container-name> ` |
|      To open bash shell inside the container. |     `$ docker exec -it <container-name> /bin/bash`  |