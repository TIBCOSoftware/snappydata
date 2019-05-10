# Getting Started with TIBCO ComputeDB on Kubernetes
Kubernetes is an open source project designed for container orchestration. TIBCO ComputeDB can be deployed on Kubernetes.

This following sections are included in this topic:

*	[Prerequisites](#prerequisites)

*	[Deploying TIBCO ComputeDB Chart in Kubernetes](#deploykubernetes)


<a id= prerequisites> </a>
### Prerequisites

The following prerequisites must be met to deploy TIBCO ComputeDB on Kubernetes:

*	**Kubernetes cluster**</br> A running Kubernetes cluster of version 1.9 or higher.

*	**Helm tool**</br> Helm tool must be deployed in the Kubernetes environment. Helm comprises of two parts, that is a client and a Tiller (Server portion of Helm) inside the kube-system namespace. Tiller runs inside the Kubernetes cluster and manages the deployment of charts or packages. You can follow the instructions [here](https://docs.pivotal.io/runtimes/pks/1-1/configure-tiller-helm.html) to deploy Helm in your Kubernetes enviroment.

*	**Docker image**</br> Helm charts use the Docker images to launch the container on Kubernetes. [You can refer to these steps](getting_started_with_docker_image.md#build-your-docker) to build your Docker image for TIBCO ComputeDB, provided you have its tarball with you. TIBCO does not provide a Docker image for TIBCO ComputeDB.

<a id= deploykubernetes> </a>
## Deploying TIBCO ComputeDB on Kubernetes 

TIBCO ComputeDB Helm chart is used to deploy TIBCO ComputeDB on Kubernetes.  It uses Kubernetes [statefulsets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) to launch the locator, lead, and server members. 

**To deploy TIBCO ComputeDB on Kubernetes:**

1.	Clone the **spark-on-k8s** repository and change to **charts** directory.</br>
`git clone https://github.com/SnappyDataInc/spark-on-k8s`</br>
`cd spark-on-k8s/charts`

3.	Optionally, go to *snappydata* > *values.yaml*  file to edit the default configurations in SnappyData chart. Configurations can be specified in the respective attributes for locators, leaders, and servers in this file. Refer [List of Configuration Parameters for SnappyData Chart](../kubernetes.md#chartparameters)

4.	Install the *snappydata* chart using the following command:</br>
`helm install --name snappydata --namespace snappy ./snappydata/`

	The above command installs the SnappyData chart in a namespace called *snappy* and displays the Kubernetes objects (service, statefulsets etc.) created by the chart on the console.</br>
    By default, TIBCO ComputeDB helm chart deploys a TIBCO ComputeDB cluster which consists of one locator, one lead, two servers and services to access TIBCO ComputeDB endpoints.

You can monitor the Kubernetes UI dashboard to check the status of the components as it takes few minutes for all the servers to be online. To access the Kubernetes UI refer to the instructions [here](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/#accessing-the-dashboard-ui). 

TIBCO ComputeDB chart dynamically provisions volumes for servers, locators, and leads. These volumes and the data in it are retained even after the chart deployment is deleted.

For more details on accessing and interacting with TIBCO ComputeDB cluster on Kubernetes refer to [TIBCO ComputeDB Cluster on Kubernetes](../kubernetes.md#interactkubernetes)
