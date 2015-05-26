export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
bin/hadoop com.sun.tools.javac.Main Censo*.java
jar cf censo.jar Censo*.class
hadoop fs -rm /user/hduser/output/*
hadoop fs -rmdir /user/hduser/output
bin/hadoop jar censo.jar CensoManager /user/hduser/input/ /user/hduser/output 0 -files /opt/hadoop/censoPoblacion.csv
rm part-r-00000 
bin/hadoop fs -copyToLocal /user/hduser/output/part-r-00000
