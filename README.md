# riobus-report-average-speed
spark project written in scala that filters buses inside a given latitude-longitude rectangle and calculates the overall average speed.

<h6>you only need to produce a jar</h6>
<ol>
    <li>install sbt (simple build tool)<br>
    more information in http://www.scala-sbt.org/0.13/tutorial/Setup.html<br>
    <li>cd to project folder <br>
    <code>$ cd path/to/project</code></li>
    <li>use pacakge command with sbt <br>
    <code>$ sbt package</code></li>
    <li>jar will be inside ./target/scala-2.10/ folder<br>
</ol>

<h6>now you need to submit this jar to spark<br>
read https://spark.apache.org/docs/latest/submitting-applications.html for more information<br>
this project has been tested on spark 1.6.1</h6>
