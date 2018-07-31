<html>
<HEAD>
</head>
<body>

<div id="readme" class="clearfix announce instapaper_body md">
<article class="markdown-body entry-content" itemprop="mainContentOfPage">

<h2><a name="welcome-to-triplegeo" class="anchor" href="#welcome-to-triplegeo"><span class="octicon octicon-link"></span></a>Welcome to TripleGeo: An open-source tool for transforming geospatial features into RDF triples</h2>

<p>TripleGeo is a utility developed by the <a href="http://www.imis.athena-innovation.gr/">Information Systems Management Institute</a> at <a href="http://www.athena-innovation.gr/en.html">Athena Research Center</a> under the EU/FP7 project <a href="http://geoknow.eu">GeoKnow: Making the Web an Exploratory for Geospatial Knowledge</a> and the EU/H2020 Innovation Action <a href="http://slipo.eu">SLIPO:  Scalable Linking and Integration of big POI data</a>. This generic purpose, open-source tool can be used for extracting features from geospatial files and databases and thransforming them into RDF triples.</p>

<p><a href="https://github.com/GeoKnow/TripleGeo">Initial releases of TripleGeo</a> were based on open-source utility <a href="https://github.com/boricles/geometry2rdf/tree/master/Geometry2RDF">geometry2rdf</a>. Starting from version 1.2, the source code has been completely re-engineered, rewritten, and further enhanced towards scalable performance against big data volumes, as well as advanced support for more input formats and attribute schemata. TripleGeo is written in Java and is still under development; more enhancements will be included in future releases. However, all supported functionality has been tested and works smoothly in both MS Windows and Linux platforms.</p>

<h2>
<a name="quick-start" class="anchor" href="#Quick start"><span class="octicon octicon-link"></span></a>Quick start</h2>

<h3>
<a name="installation" class="anchor" href="#Installation"><span class="octicon octicon-link"></span></a>Installation</h3>

<ul>
<li>TripleGeo is a command-line utility and has several dependencies on open-source and third-party, freely redistributable libraries. The <code>pom.xml</code> file contains the project's configuration in Maven.</li>
<li> <i>Special note on JDBC drivers for database connections:</i> In case you wish to extract data from a geospatially-enabled DBMS (e.g., PostGIS), either you have to include the respective <code>.jar</code> (e.g., <code>postgresql-9.4-1206-jdbc4.jar</code>) in the classpath at runtime or to specify the respective dependency in the <code>.pom</code> and then rebuild the application. </li>
<li> <i>Special note on manual installation of a JDBC driver for Oracle DBMS:</i> Due to Oracle license restrictions, there are no public repositories that provide <code>ojdbc7.jar</code> (or any other Oracle JDBC driver) for enabling JDBC connections to an Oracle database. You need to download it and install in your local repository. Get this jar from <a href="https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides">Oracle</a> and install it in your local maven repository using:<br/>
<code>
mvn install:install-file -Dfile=/<*YOUR_LOCAL_DIR*>/ojdbc7.jar -DgroupId=com.oracle -DartifactId=ojdbc7 -Dversion=12.1.0.1 -Dpackaging=jar</code></li>
<li> Starting from version 1.3, TripleGeo includes support for custom transformation of thematic attributes according to <a href="http://rml.io/">RDF Mapping language (RML)</a>. In order to enable RML conversion mode, you need to install <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/lib/RML-Mapper.jar">RML-Mapper.jar</a> specially prepared for TripleGeo execution in your local maven repository using:<br/>
<code>
mvn install:install-file -Dfile=/<*YOUR_LOCAL_DIR*>/RML-Mapper.jar -DgroupId=be.ugent.mmlab.rml -DartifactId=rml-mapper -Dversion=0.3 -Dpackaging=jar</code></li>

<li>
Building the application with maven:<br/>
<code>mvn clean package</code><br/>
results into a <code>triplegeo-1.5-SNAPSHOT.jar</code> under directory <code>target</code> according to what has been specified in the <code>pom.xml</code> file.
</li>
</ul>


<h3>
<a name="execution" class="anchor" href="#Execution"><span class="octicon octicon-link"></span></a>Execution</h3>

TripleGeo supports <i>two-way</i> transformation of geospatial features:
<ul>
<li><i>Transformation</i> of geospatial datasets from various conventional formats into RDF data. TripleGeo supports <i>mappings</i> from the attribute schema of input dataset into an ontology for RDF features that guides the transformation (i.e., creating RDF properties, constructing URIs, defining links between entities, etc.). Optionally, <i>classification</i> of input features into categories can be also performed, provided that the user specifies a (possibly hierarchical, multi-tier) <i>classification scheme</i> (e.g., possible amenities for Points of Interest, a list of road types for a Road Network).</li>
<li><i>Reverse Transformation</i> of RDF data into de facto geospatial formats (currently, .CSV and ESRI shapefiles). TripleGeo retrieves data from a graph constructed on-the-fly from the RDF data and creates records with a geometry attribute and thematic attributes reflecting the underlying ontology of the input RDF data.</li>
</ul>

<p>Explanation and usage tips for both transformation modules are given next. The current distribution (ver. 1.5) comes with dummy configuration templates <code>file_options.conf</code> for geographical files (ESRI shapefiles, CSV, GPX, KML, etc.) and <code>dbms_options.conf</code> for database contents (from PostGIS, Oracle Spatial, etc.). These files contain indicative values for the most important properties when accessing data from geographical files or a spatial DBMS. This release also includes a template <code>reverse_options.conf</code> for reconverting RDF data back into geospatial file formats. Self-contained brief instructions can guide you into the extraction and reverse transformation processes.</p>

<p>Indicative configuration files and mappings for several cases are available <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/test/conf/">here</a> in order to assist you when preparing your own.</p>

<p>In addition, custom classification schemes for OpenStreetMap data are available <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/test/classification/">here</a> and can be readily used with the provided mappings against the <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/test/data/">sample datasets</a>. </p>

<p><b> NOTE: </b> All execution commands and configurations refer to the current version (TripleGeo ver. 1.5).</p>


<h4>
<a name="transformation" class="anchor" href="#Transformation"><span class="octicon octicon-link"></span></a>A. Transformation of geospatial datasets to RDF</h4>

How to use TripleGeo in order to transform geospatial data into RDF triples:

<ul>
<li>In case that triples will be extracted from a <i>geographical file</i> (e.g., ESRI shapefiles) as specified in the user-defined configuration file in <code>./test/conf/shp_options.conf</code>, and assuming that binaries are bundled together in <code>/target/triplegeo-1.5-SNAPSHOT.jar</code>, give a command like this:</br>
<code>java -cp ./target/triplegeo-1.5-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.Extractor ./test/conf/shp_options.conf</code></li>
<li>If triples will be extracted from a <i>geospatially-enabled DBMS</i> (e.g., PostGIS), the command is essentially the same, but it specifies a suitable configuration file <code>./test/conf/PostGIS_options.conf</code> with all information required to connect and extract data from the DBMS, as well as runtime linking to the JDBC driver for enabling connections to PostgreSQL (assuming that this JDBC driver is located at <code>./lib/postgresql-9.4-1206-jdbc4.jar</code>):</br>
<code>java -cp ./lib/postgresql-9.4-1206-jdbc4.jar;./target/triplegeo-1.5-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.Extractor ./test/conf/PostGIS_options.conf</code></li>
<li>TripleGeo supports data in GML (Geography Markup Language) and KML (Keyhole Markup Language). It can also handle INSPIRE-aligned GML data for seven Data Themes (<a href="https://inspire.ec.europa.eu/Themes/Data-Specifications/2892">Annex I</a>), as well as <a href="https://inspire.ec.europa.eu/metadata/6541">INSPIRE-aligned geospatial metadata</a>. Any such transformation is performed via XSLT, as specified in the respective configuration settings (e.g., <code>./test/conf/KML_options.conf</code>) as follows:</br>
<code>java -cp ./target/triplegeo-1.5-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.Extractor ./test/conf/KML_options.conf</code></li>
</ul>

<p>Wait until the process gets finished, and verify that the resulting output files are according to your specifications.</p>

<h4>
<a name="reverse_transformation" class="anchor" href="#ReverseTransformation"><span class="octicon octicon-link"></span></a>B. Reverse Transformation from RDF to geospatial datasets</h4>

How to use TripleGeo in order to transform RDF triples into a geospatial data file:

<ul>
<li>In the configuration file, specify one or multiple files that contain the RDF triples that will be given as input to the reverse transformation process. </li>
<li>You must specify a valid SPARQL SELECT query that will be applied against the RDF graph and will fetch the resulting records. The path to the file containing this SPARQL command must be specified in the configuration. It is assumed that the user is aware of the underlying ontology of the RDF graph. If the SPARQL query is not valid, then no or partial results may be retrieved. By default, the names of the variables in the SELECT clause will be used as attribute names in the output file. </li>
<li>The current release of TripleGeo (ver. 1.5) supports .CSV delimited files and ESRI shapefiles as output formats for reverse transformation.</li>
<li>In case of ESRI shapefile as output format, make sure that all input RDF geometries are of the same type (i.e., either points or lines or polygons), because shapefiles can only support a single geometry type in a given file. </li>
<li>Once parameters have been specified in a suitable configuration file (e.g., like <code>./test/conf/shp_reverse.conf</code>), execute the following command to launch the reverse transformation process:</br>
<code>java -cp ./target/triplegeo-1.5-SNAPSHOT.jar eu.slipo.athenarc.triplegeo.ReverseExtractor ./test/conf/shp_reverse.conf</code></li>
</ul>


<h3>
<a name="input" class="anchor" href="#Input"><span class="octicon octicon-link"></span></a>Supported Geospatial Formats</h3>

<p>The current version of TripleGeo utility can access geometries from:</p>
<ul>
<li>ESRI shapefiles, a widely used file-based format for storing geospatial features.</li>
<li>Other widely used geographical file formats, including: GPX (GPS Exchange Format), GeoJSON, as well as OpenStreetMap (OSM) XML and PBF files.</li>
<li>De facto data interchange formats with geometries specified as coordinate pairs: CSV (comma separated values), JSON. </li>
<li>Geographical data stored in GML (Geography Markup Language) and KML (Keyhole Markup Language).</li>
<li>INSPIRE-aligned datasets for seven Data Themes (Annex I) in GML format: Addresses, Administrative Units, Cadastral Parcels, GeographicalNames, Hydrography, Protected Sites, and Transport Networks (Roads).</li>
<li>Several geospatially-enabled DBMSs, including: Oracle Spatial and Graph, PostGIS extension for PostgreSQL, MySQL, Microsoft SQL Server, IBM DB2 with Spatial Extender, SpatiaLite, and ESRI Personal Geodatabases in Microsoft Access format.</li>
</ul>
</ul>


<p>Sample geographic <a href="https://github.com/SLIPO-EU/TripleGeo/tree/master/test/data/">datasets</a> for testing are available in various file formats.</p>

<h3>
<a name="output" class="anchor" href="#Output"><span class="octicon octicon-link"></span></a>Supported RDF Serializations and Spatial Ontologies</h3>

<p>In terms of <i>RDF serializations</i>, triples can be obtained in one of the following formats: RDF/XML (<i>default</i>), RDF/XML-ABBREV, N-TRIPLES, N3, TURTLE (TTL).</p>
<p>Concerning <i>geospatial representations</i>, RDF triples can be exported according to these ontologies:</p>
<ul>
<li>the <a href="https://portal.opengeospatial.org/files/?artifact_id=47664">GeoSPARQL standard</a> for several geometric types (including points, linestrings, and polygons);</li>
<li>the <a href="http://www.w3.org/2003/01/geo/">WGS84 RDF Geoposition vocabulary</a> for point features;</li>
<li>the legacy <a href="http://docs.openlinksw.com/virtuoso/rdfsparqlgeospat.html">Virtuoso RDF vocabulary</a> for point features.</li>
</ul>

<p>Resulting triples are written into local files, so that they can be readily imported into a triple store that supports the respective ontology.</p>

<h2>
<a name="usecases" class="anchor" href="#usecases"><span class="octicon octicon-link"></span></a>Use Cases</h2>

<p>TripleGeo has been used to transform a large variety of geospatial datasets into RDF. Amongst them:</p>

<ul>
<li>Exposing INSPIRE-alinged geospatial data and metadata for Greece as Linked Data through a <a href="http://geodata.gov.gr/sparql/">SPARQL endpoint</a>. This has been the first attempt to build an abstraction layer on top of the <a href="http://inspire.ec.europa.eu/">INSPIRE</a> infrastructure based on <a href="http://www.opengeospatial.org/standards/geosparql">GeoSPARQL</a> concepts, thus making INSPIRE contents accessible and discoverable as linked data.</li>
<li>Exposing Points of Interest (POI) as Linked Geospatial Data through this <a href="http://geoknow-server.imis.athena-innovation.gr:11480/pois.html">SPARQL endpoint</a>. In this case, POI data extracted from <a href="https://www.openstreetmap.org/">OpenStreetMap</a> across Europe has been transformed into RDF according a comprehensive and vendor-agnostic <a href="https://github.com/SLIPO-EU/poi-data-model">OWL ontology for POI data</a>, which enables modeling and representation of multifaceted and enriched POI profiles.</li>
</ul>

<h2>
<a name="documentation" class="anchor" href="#documentation"><span class="octicon octicon-link"></span></a>Documentation</h2>

<p>All Java classes and data structures developed for TripleGeo are fully documented in this <a href="https://slipo-eu.github.io/TripleGeo/index.html">Javadoc</a>.</p>

<h2>
<a name="license" class="anchor" href="#license"><span class="octicon octicon-link"></span></a>License</h2>

<p>The contents of this project are licensed under the <a href="https://github.com/SLIPO-EU/TripleGeo/blob/master/LICENSE">GPL v3 License</a>.</p></article>

</body>
</html>
