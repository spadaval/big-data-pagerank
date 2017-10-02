javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar:com.sun.tools.javac.Main -d /home/hduser/a0/pr/ color.java

jar -cvf /home/hduser/a0/pr/color.jar *.class

rm *.class
