To test this with multiple nodes, change the intelliJ
configuration to run the program to allow for multiple instances at the same time.

## Merging Membership Information
1. nodeMap will store all the nodes that are currently in the system
2. For evey node in a received nodeMap, add it to the receiver map if it did not exist
3. Iterate the received logs:
   1. If a log is deprecated -> Skip it
   2. Otherwise:
      1. Update the logs adding this new log to the top and removing the previous occurrence
      2. Edit the nodeMap to reflect the log changes, i.e if the membershipCounter is odd, remove the node from the nodeMap, Otherwise
no need to add it (It should already be present since the received nodeMap should include this node).

