<html>
<HEAD>
</head>
<body>

<div id="readme" class="clearfix announce instapaper_body md">
<article class="markdown-body entry-content" itemprop="mainContentOfPage">

<h2><a name="welcome-to-triplegeo" class="anchor" href="#welcome-to-triplegeo"><span class="octicon octicon-link"></span></a>Welcome to TripleGeo: An open-source tool for extracting geospatial features into RDF triples</h2>

<p>TripleGeo is a utility developed by the <a href="http://www.imis.athena-innovation.gr/">Institute for the Management of Information Systems</a> at <a href="http://www.athena-innovation.gr/en.html">Athena Research Center</a> under the EU/FP7 project <a href="http://geoknow.eu">GeoKnow: Making the Web an Exploratory for Geospatial Knowledge</a> and the EU/H2020 Innovation Action <a href="http://slipo.eu">SLIPO:  Scalable Linking and Integration of big POI data</a>. This generic purpose, open-source tool can be used for integrating features from geospatial databases into RDF triples.</p>

<p><a href="https://github.com/GeoKnow/TripleGeo">Initial releases of TripleGeo</a> were based on open-source utility <a href="https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF">geometry2rdf</a>. The current version is a complete re-engineering of the source code towards scalable performance against big data volumes, as well as advanced support for more input formats and schemata. TripleGeo is written in Java and is still under development; more enhancements will be included in future releases. However, all supported features have been tested and work smoothly in both MS Windows and Linux platforms.</p>

<h3>
<a name="quick-start" class="anchor" href="#Quick start"><span class="octicon octicon-link"></span></a>Quick start</h3>

How to use TripleGeo:
<ul>
<li>TripleGeo has several dependencies on open-source and third-party, freely redistributable libraries. The <code>pom.xml</code> file contains the project's configuration in Maven.</li>
<li> <i>Special note on manual installation of the JDBC driver for Oracle:</i> Due to Oracle license restrictions, there are no public repositories that provide <code>ojdbc7.jar</code> for enabling JDBC connections to an Oracle database. You need to download it and install in your local repository. Get this jar from <a href="https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides">Oracle</a> and install it in your local maven repository using:<br/>
<code>
mvn install:install-file -Dfile=/<YOUR_LOCAL_DIR>/ojdbc7.jar -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.1 -Dpackaging=jar</code></li>
<li>
Building with maven:<br/>
<code>mvn clean package</code>
results into a <code>triplegeo-1.2-SNAPSHOT.jar</code> under directory <code>target</code> according to what has been specified in the <code>pom.xml</code> file.
</li>
<li>The current distribution comes with dummy configuration templates <code>file_options.conf</code> for geographical files (ESRI shapefiles, CSV, GPX, etc.) and <code>dbms_options.conf</code> for database contents (from PostGIS, Oracle Spatial, etc.). These files contain indicative values for the most important properties when accessing data from geographical files or a spatial DBMS. Self-contained brief instructions can guide you into the extraction process.</li>
<li>Run the jar file from the command line:</li>
<ul>
<li>In case that triples will be extracted from a geographical file (e.g., ESRI shapefiles) as specified in the user-defined configuration file in <code>./test/conf/shp_options.conf</code>, and assuming that binaries are bundled together in <code>/target/triplegeo-1.2-SNAPSHOT.jar</code>, give a command like this:</br>
<code>java -cp ./target/triplegeo-1.2-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.Executor ./test/conf/shp_options.conf</code></li>
<li>If triples will be extracted from a geospatially-enabled DBMS (e.g., PostGIS), the command is essentially the same, but specifies a suitable configuration file <code>./test/conf/PostGIS_options.conf</code> with all information required to connect and extract data from the DBMS:</br>
<code>java -cp ./target/triplegeo-1.2-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.Executor ./test/conf/PostGIS_options.conf</code></li>
</ul>
<li>Wait until the process gets finished, and verify that the resulting output file is according to your specifications.</li>
</ul>

<p>TripleGeo also supports GML datasets aligned to EU INSPIRE Directive. In this case, no configuration file need be specified, as this process is controlled via XSLT. More specifically, TripleGeo can transform into RDF triples geometries available in GML (Geography Markup Language) and KML (Keyhole Markup Language). It can also handle INSPIRE-aligned GML data for seven Data Themes (Annex I), as well as INSPIRE-aligned geospatial metadata. Assuming that binaries are bundled together in <code>/target/triplegeo-1.2-SNAPSHOT.jar</code>, you may transform such datasets as follows:</p>
<ul>
<li>In case that triples will be extracted from a GML file, give a command like this:</br>
<code>java -cp ./target/triplegeo-1.2-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.tools.GmlToRdf input.gml output.rdf </code></li>
<li>In case that triples will be extracted from a KML file, give a command like this:</br>
<code>java -cp ./target/triplegeo-1.2-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.tools.KmlToRdf input.kml output.rdf </code></li>
<li>In case that triples will be extracted from a XML file with <b>INSPIRE-aligned metadata</b>, give a command like this:</br>
<code>java -cp ./target/triplegeo-1.2-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.tools.MetadataToRdf input.xml output.rdf </code></li>
<li>In case that triples will be extracted from a GML file with <b>INSPIRE-aligned data</b>, you must first configure XSL stylesheet <code>Inspire_main.xsl</code> with specific parameters and then give a command like this:</br>
<code>java -cp ./target/triplegeo-1.2-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.tools.InspireToRdf input.gml output.rdf </code></li>
</ul>


<p>Indicative configuration files for several cases are available <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/test/conf/">here</a> in order to assist you when preparing your own.</p>

<h3>
<a name="input" class="anchor" href="#Input"><span class="octicon octicon-link"></span></a>Input</h3>

<p>The current version of TripleGeo utility can access geometries from:</p>
<ul>
<li>ESRI shapefiles, a widely used file-based format for storing geospatial features.</li>
<li>Other widely used geographical file formats, including: CSV (comma separated values), GPX (GPS Exchange Format), GeoJSON, and OpenStreetMap (OSM) XML files.</li>
<li>Geographical data stored in GML (Geography Markup Language) and KML (Keyhole Markup Language).</li>
<li>INSPIRE-aligned datasets for seven Data Themes (Annex I) in GML format: Addresses, Administrative Units, Cadastral Parcels, GeographicalNames, Hydrography, Protected Sites, and Transport Networks (Roads).</li>
<li>Several geospatially-enabled DBMSs, including: Oracle Spatial and Graph, PostGIS extension for PostgreSQL, MySQL, Microsoft SQL Server, and IBM DB2 with Spatial Extender.</li>
</ul>
</ul>

<p>Sample geographic <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/test/data/">datasets</a> for testing are available in various file formats.</p>

<h3>
<a name="output" class="anchor" href="#Output"><span class="octicon octicon-link"></span></a>Output</h3>

<p>In terms of <i>output serializations</i>, RDF triples can be obtained in one of the following formats: RDF/XML (<i>default</i>), RDF/XML-ABBREV, N-TRIPLES, N3, TURTLE (TTL).</p>
<p>Concerning <i>geospatial representations</i>, RDF triples can be exported according to:</p>
<ul>
<li>the <a href="https://portal.opengeospatial.org/files/?artifact_id=47664">GeoSPARQL standard</a> for several geometric types (including points, linestrings, and polygons)</li>
<li>the <a href="http://www.w3.org/2003/01/geo/">WGS84 RDF Geoposition vocabulary</a> for point features</li>
<li>the <a href="http://docs.openlinksw.com/virtuoso/rdfsparqlgeospat.html">Virtuoso RDF vocabulary</a> for point features.</li>
</ul>

<p>Resulting triples are written into local files, so that they can be readily imported into a triple store.</p>


<h2>
<a name="license" class="anchor" href="#license"><span class="octicon octicon-link"></span></a>License</h2>

<p>The contents of this project are licensed under the <a href="https://github.com/SLIPO-EU/TripleGeo/blob/master/LICENSE">GPL v3 License</a>.</p></article>

</body>
</html>
