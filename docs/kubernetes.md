# Getting Started with SnappyData on Kubernetes
Kubernetes is an open source project designed for container orchestration. SnappyData can be deployed on Kubernetes. This is currently available as an experimental feature.

This following sections are included in this topic:

*	[Prerequisites](#prerequisites)
*	[Deploying SnappyData Chart in Kubernetes](#deploykubernetes)
*	[Accessing SnappyData Pulse in Kubernetes](#accesskubernetes)
*	[Connecting SnappyData using JDBC Driver in Kubernetes](#jdbckubernetes)
*	[Executing Queries in Kubernetes Deployment](#querykubernetes)
*	[Submitting a SnappyData Job in Kubernetes](#jobkubernetes)
*	[List of Configuration Parameters for SnappyData Chart](#chartparameters)
*	[Deleting the SnappyData Helm Chart](#deletehelm)
*	[Kubernetes Obects Used in SnappyData Chart](#kubernetesobjects)

<a id= prerequisites> </a>
### Prerequisites

The following prerequisites must be met to deploy SnappyData on Kubernetes:

*	**Kubernetes or Pivotal Container Service (PKS) cluster**</br> A running Kubernetes cluster of version 1.9 or higher or PKS cluster of version 1.0.0 or higher.

*	**Helm tool**</br> Helm tool must be deployed in the Kubernetes environment. Helm comprises of two parts, that is a client and a Tiller (Server portion of Helm) inside the kube-system namespace. Tiller runs inside the Kubernetes cluster and manages the deployment of charts or packages.You can follow the instructions [here](https://docs.pivotal.io/runtimes/pks/1-0/configure-tiller-helm.html) to deploy Helm in your Kubernetes enviroment.


<a id= deploykubernetes> </a>
## Deploying SnappyData on Kubernetes 

SnappyData Helm chart is used to deploy SnappyData on Kubernetes.  It uses Kubernetes [statefulsets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) to launch the locator, lead, and server members. 

**To deploy SnappyData on Kubernetes:**

1.	Clone the **spark-on-k8s** repository.</br>
`git clone https://github.com/SnappyDataInc/spark-on-k8s`

2.	Change to **charts** directory.</br>
`cd charts`

3.	Optionally, go to *snappydata* > *values.yaml*  file to edit the default configurations in SnappyData chart. Configurations can be specified in the respective attributes for locators, leaders, and servers in this file. Refer [List of Configuration Parameters for SnappyData Chart](#chartparameters)

4.	Install the *snappydata* chart in the namespace *snappy* using the following command:</br>
`helm install --name snappydata --namespace snappy ./snappydata/`

	The `helm install` command deploys the SnappyData chart in a namespace called *snappy* and displays the Kubernetes objects (service, statefulsets etc.) created by the chart on the console.</br>
    By default, SnappyData helm chart deploys a SnappyData cluster which consists of one locator, one lead, two servers and services to access SnappyData endpoints.

You can monitor the Kubernetes UI dashboard to check the status of the components as it takes few minutes for all the servers to be online.

!!!Note
	SnappyData chart dynamically provisions volumes for servers, locators, and leads. These volumes and the data in it are retained even after the chart deployment is deleted.

In case the cluster or any of the cluster members fail to launch, you can access the SnappyData logs by mounting the persistent volume claims of the corresponding member (locator, servers, or lead) on a dummy container. </br>Experimental scripts are provided to do this which are available [here](https://github.com/SnappyDataInc/spark-on-k8s/blob/master/utils/snappy-debug-pod.sh).</br>
The script takes the name of the persistent volume claim that is to be mounted and mounts it on the container as */data0*, */data1*, and so on. You can find out the names of persistent volume claims on Kubernetes Dashboard or by running `kubectl get persistentvolumeclaims`. For more details, execute the script without any arguments.

<a id= accesskubernetes> </a>
## Accessing SnappyData Pulse in Kubernetes

The dashboards on the SnappyData Pulse can be accessed using `snappydata-leader-public ` service. To view the dashboard, type the URL in the web browser, using the following format:</br>
*externalIp:5050*. </br>
Replace *externalip* with the IP address used for external connections.

**To access SnappyData Pulse in Kubernetes:**

1. Check the SnappyData services running in the Kubernetes cluster.</br>
`kubectl get svc --namespace=snappy`</br> The output displays the external IP address of the *leader-public *service.

3. Type *externalIp:5050* in the browser. Here you must replace *externalip* with the external IP address of the *leader-public* service.</br> For example, 35.232.102.51:5050.

<a id= querykubernetes> </a>
## Executing Queries in Kubernetes Deployment

You  can use Snappy shell to connect to SnappyData and execute your queries. Download the SnappyData distribution from [SnappyData github releases](https://github.com/SnappyDataInc/snappydata/releases). Snappy shell need not run within the Kubernetes cluster.

**To execute queries in Kubernetes deployment:**

1.	Check the SnappyData services running in the Kubernetes cluster.</br>
`kubectl get svc --namespace=snappy`</br>
The [output](#output) displays the external IP address of the *locator-public* services  and port number for external connections.

3.	Launch snappy shell  and connect to the *locator-public* service's external IP address using the following commands:</br>
```
bin/snappy
externalURL:1527
```
The externalURL should be replaced with the IP address used for external connections. </br>For example, `snappy> connect client '104.198.47.162:1527';`

<a id= jdbckubernetes> </a>
## Connecting SnappyData Using JDBC Driver in Kubernetes

For Kubernetes deployments, JDBC clients can connect to SnappyData system using the `SnappyData-locator-public` service.

**To connect to SnappyData using JDBC driver in Kubernetes:**

1.	Check the SnappyData services running in Kubernetes cluster.</br>
`kubectl get svc --namespace=snappy`</br>
The [output](#output) displays the external IP address  of the *locator-public *service and the port number for external connections which must be noted.

2.	In the web browser, type the URL in the following format:</br>
[jdbc:snappydata://locatorIP:locatorClientPort/]() 
</br>Replace the *locatorIP* and *locatorClientPort* with the external IP address of *SnappyData-locator-public* service and the port number for external connections respectively.

You can refer to [SnappyData documentation](http://snappydatainc.github.io/snappydata/howto/connect_using_jdbc_driver/) for an example of JDBC program and for instructions on how to obtain JDBC driver using Maven/SBT co-ordinates.

<a id= jobkubernetes> </a>
## Submitting a SnappyData Job in Kubernetes

Refer to the [How Tos section](http://snappydatainc.github.io/snappydata/howto/run_spark_job_inside_cluster/) in SnappyData documentation to understand how to submit SnappyData jobs.
However, for submitting a SnappyData job in Kubernetes deployment, you need to use the `SnappyData-leader-public service` that exposes port 8090 to run the jobs.

**To submit a SnappyData job in Kubernetes deployment:**

1.	Check the SnappyData services running in Kubernetes cluster.</br>
`kubectl get svc --namespace=snappy`</br>
The [output](#output) displays the external IP address of *leader-public* service which must be noted

3.	Change to SnappyData product directory.</br>
`cd $SNAPPY_HOME`

3.	Submit the job in the **-lead** option using the external IP of `snappydata-leader-public` service and the port number **8090**.</br>For example:
```
bin/snappy-job.sh submit
--app-name CreatePartitionedRowTable
  --class org.apache.spark.examples.snappydata.CreatePartitionedRowTable
  --app-jar examples/jars/quickstart.jar
  --lead 35.232.102.51:8090
```
<a id= deletehelm> </a>
## Deleting the SnappyData Helm Chart

You can delete the SnappyData Helm chart using the `helm delete` command.

```
$ helm delete --purge snappydata
```
The dynamically provisioned volumes and the data in it is retained even if the chart deployment is deleted.

<a id= chartparameters> </a>
## List of Configuration Parameters for SnappyData Chart

You can modify the *values.yaml*  file to configure the SnappyData chart. The following table lists the configuration parameters available for this chart:

| Parameter| Description | Default |
| ---------| ------------| --------|
| `image.repository` |  Docker repo from which the SnappyData Docker image is pulled.    |  `snappydatainc/snappydata`   |
| `image.tag` |  Tag of the SnappyData Docker image that is pulled. |   |
| `image.pullPolicy` | Pull policy for the image.  | `IfNotPresent` |
| `locators.conf` | List of the configuration options that is passed to the locators. | |
| `locators.resources` | Resource configuration for the locator Pods. User can configure CPU/memory requests and limit the usage. | `locators.requests.memory` is set to `1024Mi`. |
| `locators.persistence.storageClass` | Storage class that is used while dynamically provisioning a volume. | Default value is not defined so `default` storage class for the cluster is chosen.  |
| `locators.persistence.accessMode` | [Access mode](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) that is used for the dynamically provisioned volume. | `ReadWriteOnce` |
| `locators.persistence.size` | Size of the dynamically provisioned volume. | `10Gi` |
| `servers.replicaCount` | Number of servers that are started in a SnappyData system. | `2` |
| `servers.conf` | List of the configuration options that are passed to the servers. | |
| `servers.resources` | Resource configuration for the server Pods. You can configure CPU/memory requests and limit the usage. | `servers.requests.memory` is set to `4096Mi` |
| `servers.persistence.storageClass` | Storage class that is used while dynamically provisioning a volume. | Default value is not defined so `default` storage class for the cluster will be chosen.  |
| `servers.persistence.accessMode` | [Access mode](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) for the dynamically provisioned volume | `ReadWriteOnce` |
| `servers.persistence.size` | Size of the dynamically provisioned volume. | `10Gi` |
| `leaders.conf` | List of configuration options that can be passed to the leaders. | |
| `leaders.resources` | Resource configuration for the server pods. You can configure CPU/memory requests and limits the usage | `leaders.requests.memory` is set to `4096Mi` |
| `leaders.persistence.storageClass` | Storage class that is used while dynamically provisioning a volume. | Default value is not defined so `default` storage class for the cluster will be chosen.  |
| `leaders.persistence.accessMode` | [Access mode](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) for the dynamically provisioned volume. | `ReadWriteOnce` |
| `leaders.persistence.size` | Size of the dynamically provisioned volume. | `10Gi` |


The following sample shows the configuration used to start four servers each with a heap size of 2048 MB:

```
servers:
  replicaCount: 4
  ## config options for servers
  conf: "-heap-size=2048m"
```

You can specify SnappyData [configuration parameters](https://snappydatainc.github.io/snappydata/configuring_cluster/configuring_cluster/#configuration-files) in the `servers.conf`, `locators.conf`, and `leaders.conf` attributes for servers, locators, and leaders respectively.

<a id= kubernetesobjects> </a>
## Kubernetes Obects Used in SnappyData Chart

This section provides details about the Kubernetes objects that are used in SnappyData Chart.

**Statefulsets for Servers, Leaders, and Locators**</br>
[Kubernetes statefulsets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) are used to manage stateful applications. Statefulsets provide many benefits such as stable and unique network identifiers, stable persistent storage, ordered deployment and scaling, graceful deletion, and rolling updates.
SnappyData Helm charts deploy statefulsets for servers, leaders, and locators. By default the chart creates two data servers (pods) for SnappyData servers and one pod each for locator and leader. Upon deletion of the Helm deployment, each pod gracefully terminates the SnappyData process that is running on it.

**Services that Expose External Endpoints**</br>
SnappyData Helm chart creates services to allows you to make JDBC connections, execute Spark jobs, and access
SnappyData Pulse etc.  Services of the type LoadBalancer have external IP address assigned and can be used to connect from outside of Kubernetes cluster.
To check the service created for SnappyData deployment, use command `kubectl get svc --namespace=snappy`. The following output is displayed:<br>

<a id= output> </a>
```pre
NAME                        TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)                                        AGE
snappydata-leader           ClusterIP      None            <none>           5050/TCP                                       5m
snappydata-leader-public    LoadBalancer   10.51.255.175   35.232.102.51    5050:31964/TCP,8090:32700/TCP,3768:32575/TCP   5m
snappydata-locator          ClusterIP      None            <none>           10334/TCP,1527/TCP                             5m
snappydata-locator-public   LoadBalancer   10.51.241.224   104.198.47.162   1527:31957/TCP                                 5m
snappydata-server           ClusterIP      None            <none>           1527/TCP                                       5m
snappydata-server-public    LoadBalancer   10.51.248.27    35.232.16.4      1527:31853/TCP                                 5m

```
In the above output, three services namely `snappydata-leader-public`, `snappydata-locator-public` and 
`snappydata-server-public`  of type *LoadBalancer* are created. These services have external IP addresses assigned and therefore can be accessed from outside Kubernetes. The remaining services that do not have external IP addresses are those that are created for internal use.
 
`snappydata-leader-public` service exposes port **5050** for SnappyData Pulse and port **8090** to accept SnappyData jobs.
`snappydata-locator-public` service exposes port **1527** to accept JDBC connections.

**Persistent Volumes **</br>
A pod in a SnappyData deployment has a persistent volume mounted it. This volume is dynamically provisioned and is used
to store data directory for SnappyData. On each pod, the persistent volume is mounted on path `/opt/snappydata/work`. These volumes and the data in it is retained even if the chart deployment is deleted.

!!!Note
	If the chart is deployed again with the same chart name and if the volume exists, then the existing volume is used instead of provisioning a new volume.
