##Storing and Loading JAR Files 

Application logic, which can be used by SQL functions and procedures, includes Java class files stored in a JAR file format. Storing application JAR files in SnappyData simplifies application deployment, because it reduces the potential for problems with a user’s classpath.

SnappyData automatically loads installed JAR file classes into the class loader so that you can use them in your SnappyData applications and procedures. The JAR classes are available to all members of the SnappyData distributed system, including those that join the system at a later time.

!!! Note
	Many of the topics in this section were adapted from the Apache Derby documentation source files, and are subject to the Apache license: “ pre Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. “

<mark>Check with Shirish </mark>
* **Class Loading Overview**: You store application classes, resources, and procedure implementations in SnappyData by installing one or more JAR files. After installing the JAR file, an application can then access the classes without having to be coded in a particular way.
* **Alternate Methods for Managing JAR Files**: SnappyData also provides system procedures that you can use to interactively install and manage JAR files from a client connection. Keep in mind that the procedures have certain limitations compared to using snappy-shell commands to manage JAR files.