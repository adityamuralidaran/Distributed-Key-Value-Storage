# Distributed-Key-Value-Storage
Developed a simplified version of Amazon Dynamo key-value storage system. Implemented chain replication to provide concurrency. Handled node failures while continuing to provide availability and linearizability.

## Steps To Run The Program:
1. Use python-2.7 to run the commands
2. Create AVD:
```
python create_avd.py
python updateavd.py
```
3. Start the AVD:
```
python run_avd.py 5
```
4. Starting the emulator network:
```
python set_redir.py 10000
```
5. Test the program by running the grading script along with the build APK file of the program. (The grading is done in phase as mentioned below)
```
.\simpledynamo-grading.exe app-debug_dynamo.apk
```

## Testing Phases:
1. Testing basic operations 
- This phase will test insert, query, and delete (including the special keys @ and *). This will ensure that all data is correctly replicated. It will not perform any concurrent operations or test failures.
2. Testing concurrent operations with differing keys
- This phase will test your implementation under concurrent operations, but without failure.
- The tester will use different key-value pairs inserted and queried concurrently on various nodes.
3. Testing concurrent operations with like keys
- This phase will test your implementation under concurrent operations with the same keys, but no failure.
- The tester will use the same set of key-value pairs inserted and queried concurrently on all nodes.
4. Testing one failure
- This phase will test every operation under a single failure.
- For each operation, one node will crash before the operation starts. After the operation is done, the node will recover.
- This will be repeated for each operation.
5. Testing concurrent operations with one failure
- This phase will execute various operations concurrently and crash one node in the middle of execution. After some time, the node will recover (also during execution).
6. Testing concurrent operations with repeated failures 
- This phase will crash one node at a time, repeatedly. That is, one node will crash and then recover during execution, and then after its recovery another node will crash and then recover, etc.
- There will be a brief period of time between each crash-recover sequence.
