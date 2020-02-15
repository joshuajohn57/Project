## Hadoop batch data processing project

To store Big data in hdfs and process that Big data using pyspark to obtain the necessary information for the user.


	/appveyor/build/:user/:repo


## Table of Contents
- [Requirements](#requirements)
- [Usage](#usage)
- [Reference](#reference)
- [Support](#support)
- [License](#license)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>





## Requirements
*  Virtualization product that can support a 64-bit guest OS like Oracle VM Virtual box or VMware workstation
*  Cloudera quickstart VM
	-It contains the Hadoop tools and Apache Spark essential for this project

## Usage

Detailed information to use this project can be viewed in the Description txt in the folder Description
Basic knowledge of MySQL,HIve,PySpark,sqoop is required .
To learn more visit the links provided in the reference section
In this project i have taken the NBA box score data from 2012 to 2018 and manipulated the data using Apache spark to meet my requirements
Example
 Code to find the tallest nba player

#1 Max Height
maxheight = data.groupBy().max('Height').withColumnRenamed("max(Height)","max_height")
result1 = data.join(maxheight,data.height == maxheight.max_height,"inner").select(data.playername,data.height).distinct()
result1.show()

Output



For other examples refer 

The above python code can be edited to suit your requirements


## Reference

MySQL -https://www.w3schools.com/sql/

Spark programming guide https://spark.apache.org/docs/1.6.0/programming-guide.html

Spark SQL, DataFrames and Datasets Guide https://spark.apache.org/docs/1.6.0/sql-programming-guide.html

PySpark API documentation https://spark.apache.org/docs/1.6.0/api/python/pyspark.sql.html


## Support
Reach out to me at johuajohn7057@gmail.com


## License

MIT LICENSE https://opensource.org/licenses/mit-license.php



















