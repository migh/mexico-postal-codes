# Postal Codes Analysis
***

The main purpose of this repository is to learn about Scala and Spark, and to infer some statistical information from the data and present it to the user in a nice and understandable way, that meaning web visualization.

The data source is the list from [Sepomex](http://www.sepomex.gob.mx/lservicios/servicios/) of all postal codes for the country as of November 2016. There are two files included, CPdescarga.txt in a pipe separated list of all the postal codes from Mexico, and Descrip.pdf is the description of the fields included in CPdescarga.txt.

The fields described are:

* **d_codigo:** Postal Code
* **d_asenta:** Name
* **d_tipo_asenta:** Type (Sepomex)
* **d_mnpio:** Municipality name (INEGI)
* **d_estado:** State name (INEGI)
* **d_ciudad:** City name (Sepomex)
* **d_CP:** Postal management office postal code
* **c_estado:** State key (INEGI)
* **c_oficina:** Postal management office postal code
* **c_CP:** Empty field
* **c_tipo_asenta:** Type key (Sepomex)
* **c_mnpio:** Municipality key (INEGI)
* **id_asenta_cpcons:** Settlement id (Municipality)
* **d_zona:** Settlement zone type (Urban/Rural)
* **c_cve_ciudad:** City key (Sepomex)

For any comment or question feel free to reach me: **migsarnavarro at gmail**.