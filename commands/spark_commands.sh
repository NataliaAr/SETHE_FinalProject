export SPARK_HOME="/usr/lib/spark"

$SPARK_HOME/bin/spark-shell
val textFile = sc.textFile("hdfs://10.0.2.15:8020/user/cloudera/events/**/**/**")

spark-submit --class com.nataliaar.sethefinalprojectscala.MostFrequentlyPurchasedCategories \
--master local --deploy-mode client --executor-memory 1g \
--name MostFrequentlyPurchasedCategories --conf "spark.app.id=MostFrequentlyPurchasedCategories" \
/media/sf_VM_Shared/sethe-final-project-scala-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
hdfs://10.0.2.15:8020/user/cloudera/events/**/**/** \
jdbc:mysql://localhost:3306/narinicheva root cloudera

spark-submit --class com.nataliaar.sethefinalprojectscala.MostFrequentlyPurchasedProducts \
--master local --deploy-mode client --executor-memory 1g \
--name MostFrequentlyPurchasedProducts --conf "spark.app.id=MostFrequentlyPurchasedProducts" \
/media/sf_VM_Shared/sethe-final-project-scala-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
hdfs://10.0.2.15:8020/user/cloudera/events/**/**/** \
jdbc:mysql://localhost:3306/narinicheva root cloudera

spark-submit --class com.nataliaar.sethefinalprojectscala.CountriesWithHighestMoneySpending \
--master local --deploy-mode client --executor-memory 1g \
--name CountriesWithHighestMoneySpending --conf "spark.app.id=CountriesWithHighestMoneySpending" \
/media/sf_VM_Shared/sethe-final-project-scala-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
hdfs://10.0.2.15:8020/user/cloudera/events/**/**/** \
hdfs://10.0.2.15:8020/user/cloudera/geolite2/blocks \
hdfs://10.0.2.15:8020/user/cloudera/geolite2/locations \
jdbc:mysql://localhost:3306/narinicheva root cloudera
