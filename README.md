# MillionSongs

ExtractAndSave.py
-----------------
This script will perform following steps:

1. Recursively iterate through all the folders and reads h5 file.  (Note: I am assuming all files are h5)

2. Extracts HDF5 file contents using h5py module and exports to HDFS location (MillionSongs/AllFeatures)

3. Based on some data exploration,(it's code is not part this submission) identifier fields were excluded.  Some other fields too were excluded that contained siginificant number of nulls.

4. Export features that are of qualitative value (MillionSongs/f2)

After step-4 was completed, entire H5 subset folder was removed from my laptop's linux VM

inter.py
--------
1. Intermediate step to strip off leading '(' and lagging ')' for each line.
2. Output to MillionSongs/f3

ProcessSongs.py
----------------
This script will read the data, convert the fields to targetted data-types and then perform LinearRegression (using Gradient Descent) 
Steps involved:

1. Read input data from MillionSongs/f3

2. Create RDD with each row having either float or int data-type.

3. Split data into train/test and applied aglo on train to compute Mean Squared Error.

4. Applied model to test.

My comments
-----------
I haven't experimented apache spark's mllib/ml, however if I were to do this portion using R, I would have done the following

1. Perform feature scaling on some of it's parameters.

2. Since Artist Hotness is not a category variable and is a continuous one, I would have first attempted linear regression (not gradient descent based) to compute coefficients.

3. Inspect each feature's coefficients and it's associated P-value to either include or remove it from the model.

Some of ML experiments (based in R)
-------------------------------------
https://rpubs.com/skekatpuray


Unsuccessful attempt with Java
--------------------------------

1. Was able to use code-based from the pdf assignment link https://github.com/tbertinmahieux/MSongsDB/tree/master/JavaSrc and refactor it to remove static references so as to use it in Apache Spark scala code.

2. sbt package had compiled successfully, however failed to execute. 

3. I tried to instal .so files however in vain. 

4. I have attached TrackInfo.java 
