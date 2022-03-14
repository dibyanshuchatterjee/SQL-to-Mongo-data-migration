package edu.rit.ibd.a4;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.PushOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
//		final String dbURL = "jdbc:mysql://localhost:3306/Assingment3?useCursorFetch=true";
//		final String user = "root";
//		final String pwd = "";
//		final String mongoDBURL = "None";
//		final String mongoDBName = "Assingment4";
		System.out.println("passes 31");

		System.out.println(new Date() + " -- Started");

		Connection con = DriverManager.getConnection(dbURL, user, pwd);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		// TODO 0: Your code here!

		/*
		 *
		 * Everything in MongoDB is a document (both data and queries). To create a document, I use primarily two options but there are others
		 * 	if you ask the Internet. You can use org.bson.Document as follows:
		 *
		 * 		Document d = new Document();

		 * 		d.append("name_of_the_field", value);
		 *
		 * 	The type of the field will be the conversion of the Java type of the value.
		 *
		 * 	Another option is to parse a string representing the document:
		 *
		 * 		Document d = Document.parse("{ _id:1, name:\"Name\" }");
		 *
		 * 	It will parse only well-formed documents. Note that the previous approach will use the Java data types as the types of the pieces of
		 * 		data to insert in MongoDB. However, the latter approach will not have that info as everything is a string; therefore, be mindful
		 * 		of these differences and use the approach it will fit better for you.
		 *
		 * If you wish to create an embedded document, you can use the following:
		 *
		 * 		Document outer = new Document();
		 * 		Document inner = new Document();
		 * 		outer.append("doc", inner);
		 *
		 * To connect to a MongoDB database server, use the getClient method above. If your server is local, just provide "None" as input.
		 *
		 * You must extract data from MySQL and load it into MongoDB. Note that, in general, the data in MongoDB is denormalized, which means that it includes
		 * 	redundancy. You must think of ways of extracting such redundant data in batches, that is, you should think of a bunch of queries that will retrieve
		 * 	the whole database in a format it will be convenient for you to load in MongoDB. Performing many small SQL queries will not work.
		 *
		 * If you execute a SQL query that retrieves large amounts of data, all data will be retrieved at once and stored in main memory. To avoid such behavior,
		 * 	the JDBC URL will have the following parameter: 'useCursorFetch=true' (already added by the grading software). Then, you can control the number of
		 * 	tuples that will be retrieved and stored in memory as follows:
		 *
		 * 		PreparedStatement st = con.prepareStatement("SELECT ...");
		 * 		st.setFetchSize(batchSize);
		 *
		 * where batchSize is the number of rows.
		 *
		 * Null values in MySQL must be translated as documents without such fields.
		 *
		 * Once you have composed a specific document with the data retrieved from MySQL, insert the document into the appropriate collection as follows:
		 *
		 * 		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		 *
		 * 		...
		 *
		 * 		Document d = ...
		 *
		 * 		...
		 *
		 * 		col.insertOne(d);
		 *
		 * You should focus first on inserting all the documents you need (movies and people). Once those documents are already present, you should deal with
		 * 	the mapping relations. To do so, MongoDB is optimized to make small updates of documents referenced by their keys (different than MySQL). As a
		 * 	result, it is a good idea to update one document at a time as follows:
		 *
		 * 		PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
		 * 		st.setFetchSize(batchSize);
		 * 		ResultSet rs = st.executeQuery();
		 * 		while (rs.next()) {
		 * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
		 * 			...
		 *
		 * The updateOne method updates one single document based on the filter criterion established in the first document (the _id of the document to fetch
		 * 	in this case). The second document provided as input is the update operation to perform. There are several updates operations you can perform (see
		 * 	https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to update arrays, $push and $addToSet are the best options but have slightly
		 * 	different semantics. Make sure you read and understand the differences between them.
		 *
		 * When dealing with arrays, another option instead of updating one by one is gathering all values for a specific document and perform a single update.
		 *
		 * Note that array fields that are empty are not allowed, so you should not generate them.
		 *
		 */

//		Document d = new Document();
//		d.append();
		createMovies(db,con);
//
		createPeople(db,con);



//		createmovieDenorm(db,con);

//		createpeopleDenorm(db,con);

//
//		st = con.prepareStatement("...");
//		rs = st.executeQuery();
//		while (rs.next())
//			col.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);
//		rs.close();
//		st.close();

		// TODO 0: End of your code.

		client.close();
		con.close();
	}

	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

	//creating the movies collection

	public static void createMovies(MongoDatabase db, Connection con) throws SQLException {
		MongoCollection<Document> col = db.getCollection("Movies");

		MongoCollection<Document> colForMovieDenorm = db.getCollection("MoviesDenorm");

		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement st = con.prepareStatement("SELECT * FROM movie");
		System.out.println("passed 190");
		st.setFetchSize(/* Batch size */ 200);
		System.out.println("passed 192");
		ResultSet rs = st.executeQuery();
		System.out.println("passed 194");
		while (rs.next()) {
			Document d = new Document();
			Document dForMDenorm = new Document();
			Integer _id = rs.getInt("id");
			System.out.println("166 - " + _id);
			d.append("_id", _id);
			dForMDenorm.append("_id",_id);
			System.out.println("passed 198");
			String otitle = rs.getString("otitle");
			System.out.println("otitle - " + otitle);
			if (!rs.wasNull()){
				d.append("otitle",otitle);
				System.out.println("passed 202");
			}
			String ptitle = rs.getString("ptitle");
			System.out.println("176 - " + ptitle);
			if (!rs.wasNull()){
				d.append("ptitle",ptitle);
				System.out.println("passed 207");
			}
			Boolean adult = rs.getBoolean("adult");
			System.out.println("182 - " + adult);
			if (!rs.wasNull()){
				d.append("adult",adult);
				System.out.println("passed 212");
			}
			Integer year = rs.getInt("year");
			System.out.println("188 - " + year);
			if (!rs.wasNull()){
				d.append("year",year);
				System.out.println("passed 217");
			}

			Integer runtime = rs.getInt("runtime");
			System.out.println("195 - " + runtime);
			if (!rs.wasNull()){
				d.append("runtime",runtime);
				System.out.println("passed 223");
			}

			float temp = rs.getFloat("rating");
			if (!rs.wasNull()){
			Decimal128 rating = new Decimal128(rs.getBigDecimal("rating"));
			System.out.println("202 - " + rating);

				d.append("rating",new Decimal128(new BigDecimal(rating.toString())));
				System.out.println("passed 229");
			}

			Integer totalvotes = rs.getInt("totalvotes");
			System.out.println("209 - " + totalvotes);
			if (!rs.wasNull()){
				d.append("totalvotes",totalvotes);
				System.out.println("passed 236");
			}

			System.out.println("passed 219");
			col.insertOne(d);
			colForMovieDenorm.insertOne(dForMDenorm);
		}
		System.out.println("passed 222");


		/**
		 * creating genre arrays
		 * */




		rs.close();
		st.close();
		addingGenresArray(col,con);

		addingMovieDenormActorArray(colForMovieDenorm,con);
		addingMovieDenormDirectorArray(colForMovieDenorm,con);
		addingMovieDenormProducerArray(colForMovieDenorm,con);
		addingMovieDenormWriterArray(colForMovieDenorm,con);


	}

	public static void createPeople(MongoDatabase db, Connection con) throws SQLException {


		MongoCollection<Document> col = db.getCollection("People");
		MongoCollection<Document> colForPDenorm = db.getCollection("PeopleDenorm");
		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement st = con.prepareStatement("SELECT * FROM person");
		System.out.println("passed 132");
		st.setFetchSize(/* Batch size */ 200);
		System.out.println("passed 134");
		ResultSet rs = st.executeQuery();
		System.out.println("passed 136");
		while (rs.next()) {
			Document d = new Document();
			Document dForPDneorm = new Document();
			Integer _id = rs.getInt("id");
			d.append("_id", _id);
			dForPDneorm.append("_id", _id);
			System.out.println("passed 133");
			String name = rs.getString("name");
			if (!rs.wasNull()){
				d.append("name",name);
				System.out.println("passed 137");
			}
			Integer byear = rs.getInt("byear");
			if (!rs.wasNull()){
				d.append("byear",byear);
				System.out.println("passed 142");
			}
			Integer dyear = rs.getInt("dyear");
			if (!rs.wasNull()){
				d.append("dyear",dyear);
				System.out.println("passed 147");
			}
			// If something is NULL, then, do not include the field!

			// To deal with float attributes, use the code below to retrieve big decimals for attribute x in MySQL and create Decimal128 in MongoDB.
//			Decimal128 x = new Decimal128(rs.getBigDecimal("x"));
//			x.toString();
			System.out.println("passed 148");
			col.insertOne(d);
			colForPDenorm.insertOne(dForPDneorm);
		}
		System.out.println("passed 150");

		rs.close();
		st.close();

		addingPeopleDenormActedArray(colForPDenorm,con);
		addingPeopleDenormDirectedArray(colForPDenorm,con);
		addingPeopleDenormKnownForArray(colForPDenorm,con);
		addingPeopleDenormProducedArray(colForPDenorm,con);
		addingPeopleDenormWrittenArray(colForPDenorm,con);

	}

	public static void createmovieDenorm(MongoDatabase db, Connection con) throws SQLException {

		MongoCollection<Document> col = db.getCollection("MoviesDenorm");
		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement st = con.prepareStatement("SELECT * FROM movie");

		st.setFetchSize(/* Batch size */ 200);

		ResultSet rs = st.executeQuery();
		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("id");

			d.append("_id", _id);

			col.insertOne(d);
		}




		rs.close();
		st.close();
		addingMovieDenormActorArray(col,con);
		addingMovieDenormDirectorArray(col,con);
		addingMovieDenormProducerArray(col,con);
		addingMovieDenormWriterArray(col,con);

	}

	public static void createpeopleDenorm(MongoDatabase db, Connection con) throws SQLException {

		MongoCollection<Document> col = db.getCollection("PeopleDenorm");
		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement st = con.prepareStatement("SELECT * FROM person");

		st.setFetchSize(/* Batch size */ 200);

		ResultSet rs = st.executeQuery();
		int c = 0;
		while (rs.next()) {
			Document d = new Document();
			System.out.println("int popple denorm");
//			Integer _id = rs.getInt("id");

			d.append("_id", rs.getInt("id"));
			System.out.println("before pd insert - " + ++c);
			col.insertOne(d);
		}



		/**
		 * creating genre arrays
		 * */




		rs.close();
		st.close();

		addingPeopleDenormActedArray(col,con);
		addingPeopleDenormDirectedArray(col,con);
		addingPeopleDenormKnownForArray(col,con);
		addingPeopleDenormProducedArray(col,con);
		addingPeopleDenormWrittenArray(col,con);

//		addingMovieDenormActorArray(col,con);
//		addingMovieDenormDirectorArray(col,con);
//		addingMovieDenormProducerArray(col,con);
//		addingMovieDenormWriterArray(col,con);

	}

	public static void addingPeopleDenormActedArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT *\n" +
				"FROM person as p\n" +
				"JOIN actor AS a ON a.pid = p.id");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("p.id");

			//making the actor array
			Integer acted = rs.getInt("a.mid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("acted", acted))
			);

		}
		rs.close();
		st.close();


	}

	public static void addingPeopleDenormDirectedArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT *\n" +
				"FROM person as p\n" +
				"JOIN director AS d ON d.pid = p.id");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("p.id");

			//making the actor array
			Integer directed = rs.getInt("d.mid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("directed", directed))
			);

		}
		rs.close();
		st.close();


	}


	public static void addingPeopleDenormKnownForArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT *\n" +
				"FROM person as p\n" +
				"JOIN knownfor AS k ON k.pid = p.id");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("p.id");

			//making the actor array
			Integer knownfor = rs.getInt("k.mid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("knownfor", knownfor))
			);

		}
		rs.close();
		st.close();


	}


	public static void addingPeopleDenormProducedArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT *\n" +
				"FROM person as p\n" +
				"JOIN producer AS pr ON pr.pid = p.id");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("p.id");

			//making the actor array
			Integer produced = rs.getInt("pr.mid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("produced", produced))
			);

		}
		rs.close();
		st.close();


	}

	public static void addingPeopleDenormWrittenArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT *\n" +
				"FROM person as p\n" +
				"JOIN writer AS w ON w.pid = p.id");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("p.id");

			//making the actor array
			Integer written = rs.getInt("w.mid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("written", written))
			);

		}
		rs.close();
		st.close();


	}


	public static void addingMovieDenormActorArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT * \n" +
				"FROM movie As m\n" +
				"JOIN actor AS a ON a.mid = m.id\n" +
				"\n");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("m.id");

			//making the actor array
			Integer actor = rs.getInt("a.pid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("actors", actor))
			);

		}
		rs.close();
		st.close();


	}

	public static void addingMovieDenormDirectorArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT * \n" +
				"FROM movie As m\n" +
				"JOIN director AS d ON d.mid = m.id\n" +
				"\n");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("m.id");

			//making the actor array
			Integer director = rs.getInt("d.pid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("directors", director))
			);

		}
		rs.close();
		st.close();


	}


	public static void addingMovieDenormProducerArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT * \n" +
				"FROM movie As m\n" +
				"JOIN producer AS p ON p.mid = m.id\n" +
				"\n");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("m.id");

			//making the actor array
			Integer producers = rs.getInt("p.pid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("producers", producers))
			);

		}
		rs.close();
		st.close();


	}

	public static void addingMovieDenormWriterArray(MongoCollection<Document> col, Connection con) throws SQLException{
		PreparedStatement st = con.prepareStatement("SELECT * \n" +
				"FROM movie As m\n" +
				"JOIN writer AS w ON w.mid = m.id\n" +
				"\n");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();

		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("m.id");

			//making the actor array
			Integer writers = rs.getInt("w.pid");

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("writers", writers))
			);

		}
		rs.close();
		st.close();


	}



	public static void addingGenresArray(MongoCollection<Document> col, Connection con) throws SQLException {

		PreparedStatement st = con.prepareStatement("SELECT m.id, m.ptitle,m.otitle,m.adult,m.year,m.runtime,m.rating,m.totalvotes,g.name \n" +
				"FROM movie as m\n" +
				"JOIN moviegenre as mg ON mg.mid = m.id\n" +
				"JOIN genre as g ON g.id = mg.gid");

		st.setFetchSize(/* Batch size */ 200);
		ResultSet rs = st.executeQuery();
		while (rs.next()) {
			Document d = new Document();
			Integer _id = rs.getInt("m.id");
			String name = rs.getString("g.name");

			//> db.student.update( { "subjects" : "gkn" },{ $push: { "achieve": 95 } });


//			PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
//		 * 		st.setFetchSize(batchSize);
//		 * 		ResultSet rs = st.executeQuery();
//		 * 		while (rs.next()) {
//		 * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
//		 * 			...


//			col.updateOne(Document.parse("{ _id : "+_id+" }"), Document.parse("{ $push: { genres : "+name+" } }"));

			col.updateOne(
					Filters.eq("_id", _id),
					d.append(
							"$push",
							new Document("genres", name))
					);


		}
		rs.close();
		st.close();

	}

}
