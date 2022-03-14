# SQL-to-Mongo-data-migration
Use of the gradle project is put to migrate the data from an existing SQL database's data to MongoDB.

Below here is the structure of the database in MongoDb to which the data was ultimately migrated : 

Movies 
{
_id : … ,
otitle : … ,
ptitle : … ,
adult : … ,
year : … ,
runtime : … ,
rating : … ,
totalvotes : … ,
genres : [ … ]
}

People
{
_id : … ,
name : … ,
byear : … ,
dyear : …
}

MoviesDenorm 
{
_id : … ,
actors : [ … ],
directors : [ … ],
producers : [ … ],
writers : [ … ]
}

PeopleDenorm
{
_id : … ,
acted : [ … ] ,
directed : [ … ] ,
knownfor : [ … ] ,
produced : [ … ] ,
written : [ … ]
}
