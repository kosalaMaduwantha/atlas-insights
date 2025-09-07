make-dirs:
	docker exec -i namenode bash -c "hdfs dfs -mkdir -p $(dst)"

make-dirs-ls:
	docker exec -i namenode bash -c "hdfs dfs -ls $(dst)"

delete-dirs:
	docker exec -i namenode bash -c "hdfs dfs -rm -r $(dst)"

upload-input:
	docker exec -i namenode bash -c "hdfs dfs -put $(src) $(dst)"

make-output:
	docker exec -i namenode bash -c "hdfs dfs -mkdir $(dst)"

execute-job:
	docker exec -i namenode bash -c "hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -input /users/kosala/input -output /users/kosala/output -mapper "python3 preprocess_mapper.py" -file /opt/hadoop/resources/python-programs/preprocess-data/preprocess_mapper.py"

list-jobs-all:
	docker exec -i namenode bash -c "hadoop job -list all"

read-exec-log:
	docker exec -i namenode bash -c "yarn logs -applicationId $(app_id)"

read-output:
	docker exec -i namenode bash -c "hdfs dfs -cat $(dst)/part-*"

read-dir-log:
	docker exec -i namenode bash -c "hdfs dfs -cat /tmp/logs/$(app_id)/*"