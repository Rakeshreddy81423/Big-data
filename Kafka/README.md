In this assignment we try to produce data from restaurant_orders.csv to the kafka topic using producer program.<br>
And retrieve those data records from  kafka topic using consumer program.

Let us see what exactly each file do.<br>
### producer.py<br>
 1.) Read the data from restaurant_orders.csv file using pandas.<br>
 2.) Flush each record to the partitions created in kafka topic.<br>
 
### consumer1.py and consumer2.py
 1.) Get the records from partitions.<br>
 2.) Both are having different group ids<br>
 **Observation:**<br>
 ==> If wew have group id's same they share the records which are produced into the partitions.<br>
      For example total records produced are 100. consumer1 can get 60 and consumer2 can get 40.<br>
 ==> If group id's are different then each consumer consumes all the produced records individually.<br>
 
 ### consumer_records_to_output_file.py <br>
 1.) In this file we try to write the records which are consumed into a csv file.<br>
 2.) Using pandas and DataFrame append each record to the outputfile when it is consumed from the partition.<br>
 
