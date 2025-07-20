make-dirs:
	docker exec -i namenode bash -c "hdfs dfs -mkdir -p /users/kosala/input"

make-dirs-ls:
	docker exec -i namenode bash -c "hdfs dfs -ls /users/kosala/input"

upload-input:
	docker exec -i namenode bash -c "hdfs dfs -put $(src) $(dst)"

make-output:
	docker exec -i namenode bash -c "hdfs dfs -mkdir $(dst)"

