# Spark Word Frequency Rollup

Spark v2.2 app for word frequency calculations per file and rolled up sum at all file level.

- To build: `sbt package`
- To test: `sbt test`
Requires spark v2.2
- To run: `spark-submit improvedigitalspark_2.11-1.0.jar absolute_filepath1 absolute_filepath2 ...`