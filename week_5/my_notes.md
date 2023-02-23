
### Installing Java

Ensure Brew and Java installed in your system:

```bash
xcode-select –install
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew install java
```

Add the following environment variables to your `.bash_profile` or `.zshrc`:
```bash
nano ~/.zshrc  
```

```bash
export JAVA_HOME=/usr/local/Cellar/openjdk/19.0.2
export PATH="$JAVA_HOME/bin/:$PATH"
```
`control+X`



Make sure Java was installed to `/usr/local/Cellar/openjdk@11/11.0.12`: Open Finder > Press Cmd+Shift+G > paste "/usr/local/Cellar/openjdk@11/11.0.12". If you can't find it, then change the path location to appropriate path on your machine.

### Installing Spark

1. Install Scala

```bash
brew install scala
```

2. Install Apache Spark

```bash
brew install apache-spark
```

3. Add environment variables: 

Add the following environment variables to your `.bash_profile` or `.zshrc`

```bash
nano ~/.zshrc  
```

```bash
export SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.1/libexec
export PATH="$SPARK_HOME/bin/:$PATH"
```
`control+X`

### Testing Spark

Execute `spark-shell` and run the following in scala:

```scala
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```


### PySpark
```bash
cd 
python3
import pyspark
```

+ Problem:
>ModuleNotFoundError: No module named 'py4j'
+ Solution:
```
pip install py4j
```

Findspark is a package that lets you declare the home directory of PySpark and lets you run it from other locations if your folder paths aren’t properly synced.
To install findspark, run:

```
pip3 install findpark
```

To use it in a python3 shell (or Jupyter Notebook), run the following:

```
 import findspark
 findspark.init('Users/vanaurum/server/spark-2.4.3-bin-hadoop2.7')
 import pyspark
```

https://kevinvecmanis.io/python/pyspark/install/2019/05/31/Installing-Apache-Spark.html

```python
from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

# create a dummy dataframe
data = [("Alice", 23), ("Bob", 32), ("Charlie", 45)]
df = spark.createDataFrame(data, ["Name", "Age"])

# print the dataframe
df.show()

```




