## Instruction to run samples:
We provide Maven script for you to run samples. When you run samples, you need to provide the profile ID so that Maven knows which sample you want to run.
* Go to one directory of those samples that has a pom file. for example, \src\samples\connections
* Run `mvn install -P%profileID%` and `mvn exec:java -P%profileID%`. These compile and run one of the samples under this directory. %profileID% is in the POM file.
	* For example, `mvn install -PconnectURL` then `mvn exec:java -PconnectURL` will run the "connectURL" sample at \src\samples\connections\src\main\java
