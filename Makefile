upload-input:
	docker exec -it namenode -- bash -c "hdfs dfs -put $(src) $(dst)"

make-output:
	docker exec -it namenode -- bash -c "hdfs dfs -mkdir $(dst)"
	
