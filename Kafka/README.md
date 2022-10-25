In this assignment we try to produce data from restaurant_orders.csv to the kafka topic using producer program. 
And retrieve those data records from  kafka topic using consumer program.

Let us see what exactly each file do.
### producer.py
 1.) Read the data from restaurant_orders.csv file using pandas.
 2.) Flush each record to the partitions created in kafka topic.
 
