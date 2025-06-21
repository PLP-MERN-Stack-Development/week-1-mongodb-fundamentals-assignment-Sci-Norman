const { MongoClient } = require("mongodb");

const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri);

async function run() {
  try {
    await client.connect();
    const db = client.db("plp_bookstore"); 
    const books = db.collection("books");

    // ------------------------------
    console.log("Task 2: Basic Queries");

    console.log("\n1. All Books:");
    console.log(await books.find().toArray());

    console.log("\n2. Books by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    console.log("\n3. Books published after 1950:");
    console.log(await books.find({ published_year: { $gt: 1950 } }).toArray());

    console.log("\n4. Fiction books:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    console.log("\n5. In-stock books:");
    console.log(await books.find({ in_stock: true }).toArray());

    // ------------------------------
    console.log("\nTask 3: Advanced Queries");

    console.log("\n1. In-stock books published after 2010:");
    console.log(
      await books.find({
        in_stock: true,
        published_year: { $gt: 2010 },
      }).project({ title: 1, author: 1, price: 1, _id: 0 }).toArray()
    );

    console.log("\n2. Sort books by price (ascending):");
    console.log(
      await books.find()
        .sort({ price: 1 })
        .project({ title: 1, author: 1, price: 1, _id: 0 })
        .toArray()
    );

    console.log("\n3. Sort books by price (descending):");
    console.log(
      await books.find()
        .sort({ price: -1 })
        .project({ title: 1, author: 1, price: 1, _id: 0 })
        .toArray()
    );

    console.log("\n4. Pagination - Page 1 (limit 5):");
    console.log(await books.find().skip(0).limit(5).toArray());

    console.log("\n5. Pagination - Page 2 (limit 5):");
    console.log(await books.find().skip(5).limit(5).toArray());

    // ------------------------------
    console.log("\nTask 4: Aggregation Pipeline");

    console.log("\n1. Average price of books by genre:");
    console.log(await books.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray());

    console.log("\n2. Author with the most books:");
    console.log(await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log("\n3. Books grouped by publication decade:");
    console.log(await books.aggregate([
      {
        $group: {
          _id: { $floor: { $divide: ["$published_year", 10] } },
          count: { $sum: 1 }
        }
      },
      {
        $project: {
          decade: { $multiply: ["$_id", 10] },
          count: 1,
          _id: 0
        }
      },
      { $sort: { decade: 1 } }
    ]).toArray());

    // ------------------------------
    console.log("\nTask 5: Indexing");

    console.log("\n1. Creating index on title...");
    await books.createIndex({ title: 1 });

    console.log("2. Creating compound index on author and published_year...");
    await books.createIndex({ author: 1, published_year: 1 });

    console.log("\n3. Performance check with explain():");
    const explanation = await books.find({ title: "1984" }).explain("executionStats");
    console.log(JSON.stringify(explanation.executionStats, null, 2));

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
  }
}

run();
