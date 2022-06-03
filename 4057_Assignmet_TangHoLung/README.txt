Step:
1.	open winSCP and login
2.	put the Assignment.jar in to winSCP
3.	open PuTTY and login
4.	scp Assignment.jar YourAcc@csr51:
5. 	scp bible.txt YourAcc@csr51:
6.	ssh csr51
7.	hadoop fs –copyFromLocal bible.txt ~
8.	export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/comp/YourAcc

To open different questions:
-----------------------------------------------------------------------
Case Q1.
9.	hadoop jar ./Assignment.jar Assignment ~/bible.txt ~/outputQ1
10.	hadoop fs -cat outputQ1/*
-----------------------------------------------------------------------
Case Q2.
9.	hadoop jar ./Assignment.jar AssignmentQ2 ~/bible.txt ~/outputQ2
10.	hadoop fs -cat outputQ2/*
-----------------------------------------------------------------------
Case Q3.
9.	hadoop jar ./Assignment.jar AssignmentQ3 ~/bible.txt ~/outputQ3
10.	hadoop fs -cat outputQ3/*
-----------------------------------------------------------------------
Case Q4.
9.	hadoop jar ./Assignment.jar AssignmentQ4 ~/bible.txt ~/outputQ4
10.	hadoop fs -cat outputQ4/*
-----------------------------------------------------------------------