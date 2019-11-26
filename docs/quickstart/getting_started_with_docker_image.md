<a id="getting-started-with-docker-image"></a>
# Getting Started with Docker Image

The following instructions outline how to build a Docker image if you have the binaries of TIBCO ComputeDB.</br>

!!!Note
	TIBCO ComputeDB does not provide a Docker image. You must build it explicitly.

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
## Building Docker Image of TIBCO ComputeDB</br>

You can use the Dockerfile that is provided and create your own Docker image of TIBCO ComputeDB. Download the [Dockerfile](https://github.com/SnappyDataInc/snappy-cloud-tools/blob/master/docker/Dockerfile) script and place it into a directory. The Dockerfile contains a link to the latest TIBCO ComputeDB OSS version to build the image.

Move into the directory containing the downloaded Dockerfile and then run the Docker build command with the required details to build the Docker image. 

<a id="builddockerimagesnappy"></a>
<a id="builddockerimageslocal"></a>
### Building Image from Local Copy of TIBCO ComputeDB Product TAR file

If you have already downloaded the TIBCO ComputeDB tarfile locally onto your machine, use the following steps to build an image from the downloaded binaries. To download TIBCO ComputeDB, refer to the [Provisioning TIBCO ComputeDB](https://snappydatainc.github.io/snappydata/install/) section in the product documentation.

Copy the downloaded **tar.gz** file to the Docker folder where you have placed the Dockerfile and run the following command:

```
$ docker build -t <your-docker-repo-name>/<image_name>[:<image-tag>] . --build-arg TARFILE_LOC=<tarfile name>

```

For example:

```
$ docker build -t myrepo/computedb . --build-arg TARFILE_LOC=computedb-1.1.1-bin.tar.gz
```


## Verifying Details of Docker Images

After the Docker build is successful, you can check the details for Docker images using the `docker images` command.

For example:

```
$ docker images

```

## Publishing Docker Image

If you want to publish the Docker image onto the Docker hub, login to the Docker account using `docker login` command, and provide your credentials. For more information on Docker login, visit [here](https://docs.docker.com/engine/reference/commandline/login). After a successful login, you can publish the Docker image using the `docker push` command.

```
$ docker push <your-docker-repo-name>/<image_name>[:<image-tag>]
```
Ensure to use the same name in the `docker push` that is used in `docker build`.

For example:

```
$ docker push myrepo/computedb
```
!!!Note
	This example only showcases how to push an image onto Docker Hub. You can also publish the image to other container registries such as [gcr.io](http://gcr.io). For publishing on gcr.io, you can refer [this document](https://cloud.google.com/container-registry/docs/pushing-and-pulling).

## Launching TIBCO ComputeDB Inside Docker

In the command prompt, execute the following commands to launch the TIBCO ComputeDB cluster in a single container.

```
$ docker run -itd --net=host --name <container-name> <your-docker-repo-name>/<image_name>[:<image-tag>] start all

# -i: keep the STDIN open even if not attached.
# -t: Allocate pseudo-TTY.
# -d: Detach and run the container in background and print container ID.
# --net=host: Use the Docker host network stack.
```

If the image is not available locally, this fetches the Docker image from the Docker registry, launches a default cluster consisting of one data node, one lead, and one locator in a container.

### For Linux,

```
$ docker run -itd --net=host --name computedb myrepo/computedb start all

```

### For Mac OS

!!!Note
	If you are using MAC OS, you must redirect the ports manually. If you use `--net=host`, it may not work correctly on the Mac OS. You can use the following modified command for Mac OS:

```
$ docker run -d --name=computedb -p 5050:5050 -p 1527:1527 -p 1528:1528 myrepo/computedb start all -hostname-for-clients=<Machine_IP/Public_IP>
```

The `-hostname-for-clients` parameter sets the IP Address or Hostname that the server listens for client connections. The command may take some time to execute.


## Commonly used Docker Commands

| Description| Docker Commands |
|--------|--------|
|      To check details of all the Docker containers  |     `$ docker ps -a `  |
|      To check the Docker Logs  |     `$ docker logs <container-name>`   |
|      To connect TIBCO ComputeDB with the command line client.  |     `$ docker exec -it <container-name> ./bin/snappy`   |
|     To launch a Spark shell.|     `$ docker exec -it <container-name> bin/spark-shell `  |
|      To stop the Cluster.  |     `$ docker exec -it <container-name> ./sbin/snappy-stop-all.sh `  |
|      To stop the Container.  |     `$ docker stop <container-name> ` |
|      To run commands inside the container. |     `$ docker exec -it <container-name> /bin/bash`  |
