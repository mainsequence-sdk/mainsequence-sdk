
Guidelines for this tutorial:

-Should explain Simple Tables in detail with an example in line with the rest of the tutorial.
- Should point to tutoria/simple_tables/.. and create there a deeper extample. 


Introduction:
The introduction shoudl explain what simple tables are and how they work.
Simple tablesits a minimal helper for creating tables that  doesnt fall withing hte time_index, unique_identifier normalization
of Data Nodes. think for example in an asset master list where we will just have a single table with one row
per asset unique_identifier and other tables depending on them.  coould also be for client recrods where time is not
the main index but just a property.  For this reason we have create_simple tables. 

simple tables follow the same composable structure as Data Nodes. Thye accept a configuration that defines the process that generates
the table but in this case they also need an apri-defintion of the schema. 

this allow us to define the Simple Tbale update process on two paths. 
- sistematicaly via the update method, for example by linking an extranlar data soruce inot main squence
```psedudo conde
read external data -> verify if records exists in main seuqnec otherwise insner
```
- or dyanmiacally  via an application or a dashboard (user  just want to insert, edir or delete records)


So lets start biulding th efirst simple table relation (use example simple table and explain step by step)


-Mention that we can filter and use filetring examples



conclude saying that we can mix and interact simple tables with data nodes in data pipelies by make 
asimple table te[ende of a data node] or the othwer way ardoun






