# Four-node Graphlets Enumeration Using Single Machine

This repository contains Java implementations for enumerating 4-node graphlets in undirected graphs. The details are described in the following paper:

Y. Santoso, V. Srinivasan, A. Thomo, Efficient Enumeration of Four Node Graphlets at Trillion-Scale. In the 23th International Conference on Extending Database Technology (EDBT 2020).

## The codes:

* SortGraphAsc.java - Sort the graph.

* SortGraphAscBg.java - Sort and filter the graph.

* FourGraphlets.java - Enumerate 4-node graphlets.

## Dependency

This requires:

* Java version 8 or higher.

* The WebGraph library.

## Input

The input graphs are in WebGraph format.

Available datasets in this format can be found in: <http://law.di.unimi.it/datasets.php>

There are three files needed:

*basename.graph* 

*basename.properties* 

*basename.offsets*

The first two files can be downloaded. The "offsets" file can be created by running: 
```
java -cp "lib/*" it.unimi.dsi.webgraph.BVGraph -o -O -L cnr-2000
```
We also need the transpose graph and use both to create undirected graph. After that, use SortGraphAsc and SortGraphAscBg to create sorted graphs (-ascP and -ascBg respectively). See the instructions embedded in the Java files.



