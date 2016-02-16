Cascading Job to read a text file and change the letter case from lower to upper case

To run this program, follow the below steps,

1. Clone this repository

2. Then build the jar by running mvn package and copy it into hadoop installed directory[/usr/local/hadoop/userLib]. Provide appropriate file permissions

3. Then login as Hadoop user and go to Hadoop directory[/usr/local/hadoop]

4. Create a input text file with some sentences[input.txt] and copy that file to hadoop by running the below command,

      bin/hadoop fs -copyFromLocal input.txt /example/input.txt

5. Run the jar
    bin/hadoop jar userLib/cascading-examples3-1.0-jar-with-dependencices.jar /example/input.txt /example/output
