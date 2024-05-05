This is a project to run web graphs on Spark using hadoop cluster <br>
The project has been done by 4th year students of IIIT Dharwad <br>
Team Members: <br>
Ayush Singh <br>
Avinash Tiwari and <br>
Nachiket Ganesh Apte <br>

To use the project, setup hadoop cluster of size of your choice. <br>
Note - install java 8 as hadoop doesn't suport modern versions of java. <br>
Then clone this repository and open it in intellij or similar ide which support java with maven. <br>
Now, modify the POM.xml file and update the hadoop and spark version according to your versions. Other dependencies also need to be updated accordingly. <br>

next, download the required datasets and remove any information from it except the adjacency list. <br>
now, change paths in the classes accordingly to indicate your datasets <br>

now, run mvn clean package to build the jar file of all the classes. <br>
after that run the spark submit command as given in the commands_used.txt. <br>
Note - don't forget to change the path of the jar file accordingly. <br>
 <br>
<br>
now, launch your browser and type master:8088 (where master represents the ip of your master node)  <br>
Also open master:9870 to view status of no of live nodes, files stored in hdfs file system, etc.  <br>
upon submitting, you can see the application in master:8088 with its status.<br>
