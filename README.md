# NCT-DB-MR: NCTracer Batch Jobs

## Include ImageJ filters in a Hadoop MapReduce application
-----
Given a HDFS directory of tiles in JPEG format, apply a selected ImageJ filter (e.g., Gaussian Blur 3D).
Solution:
* Make each mapper process one whole file (tile) at a time, using Hadoop's WholeFileInputFormat. The source code is in package "hadoop.wholefile".
  + If we have a lot of small files, there may be performace issues due to too many per-file overhead. 
  + This can be solved by using CombineFileInuptFormat, which needs custom extension and record reader. This can be found in package "hadoop.combinefile".
* Each map function call gets the byte array of a tile (jpeg image), and transform the bytes into a java image and apply the selected ImageJ filter.
* The filtered image is written to Database (or HDFS). In the full pipeline, the filtering step is for auto-tracing, and the filtered images do not need to be stored. For demo purposes, we write the filtered images to Database and then the NCT-Web can display them. 

## Run MatLab-exported jar in a Hadoop MapReduce applications
------
Solution:
* Use JAVA reflection technique. 
* Each worker machine must have Matlab Runtime installed. 
