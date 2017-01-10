Compile the code into Uber jar
-----------------------------------------------

    sbt assembly

The Jar will be created as spark-prediction-assembly-1.0.jar at [project-root-dir]/target/scala-2.11/

Run the jar
-----------------------------------------------

    /Developer/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --driver-memory 4G --executor-memory 4G \
    --master local \
    --class com.datalogs.main.Main [project-root-dir]/target/scala-2.11/spark-prediction-assembly-1.0.jar \
    --csv-dir "[input-data-dir]" \
    --feature-dir "[dir-to-store-constructed-features]" \
    --pipeline-stage 3

The command line parameters to run these program are -

    1.   "**--driver-memory**" and "**--executor-memory**" are optional and find details at http://spark.apache.org/docs/latest/configuration.html.

    2.   "**--master**" is required and can be set at "**local**" for local installation of spark or "**yarn**" (e.g. --master yarn)

    3.   "**--class**" is required to specify the fully-qualified-name of the "main" method in JAR and the location of the mortality_prediction-assembly-1.0.jar file.

    4.   "**--csv-dir**" is required to specify the location of MIMIC and other input files as required by this program.
            e.g. --csv-dir "[sourcecode]/MIMIC/input" (please don't put "/" at the end)

    5.   "**--feature-dir**" is required to specify the location of the program generated features file(s).
            e.g. --feature-dir "[sourcecode]/features" (please don't put "/" at the end)

    6.   "**--pipeline-stage**" is required to specify the workflow steps that the program will execute. There are 4 different options -

             --pipeline-stage 1 => For constructing features only.
             --pipeline-stage 2 => To run the all the models using the feature previous created
             --pipeline-stage 3 => To construct features and run models in one step (together)
