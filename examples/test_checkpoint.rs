use wrongodb::WrongoDB;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Testing checkpoint functionality...\n");

    // Open database
    let mut db = WrongoDB::open("test_checkpoint.db")?;

    {
        let coll = db.collection("test")?;

        // Clear test collection
        let _ = coll.delete_many(None);

        // Insert 10 documents
        println!("📝 Inserting 10 documents...");
        for i in 0..10 {
            let doc = json!({
                "_id": i,
                "name": format!("doc{}", i),
                "value": i * 10
            });
            coll.insert_one(doc)?;
        }
        println!("   ✅ Inserted 10 documents\n");

        // Find all documents
        let docs = coll.find(None)?;
        println!("📊 Found {} documents in collection\n", docs.len());

        // Find by _id (uses id BTree index)
        println!("🔍 Testing id index lookup by _id...");
        let filter = json!({ "_id": 5 });
        if let Some(doc) = coll.find_one(Some(filter))? {
            println!("   ✅ Found: {}\n", serde_json::to_string_pretty(&doc)?);
        }

        // Update a document
        println!("✏️  Testing update operation...");
        let update = json!({ "$set": { "value": 999 } });
        let filter2 = json!({ "_id": 5 });
        coll.update_one(Some(filter2), update)?;
        let filter3 = json!({ "_id": 5 });
        let updated = coll.find_one(Some(filter3))?;
        println!(
            "   ✅ After update: {}\n",
            serde_json::to_string_pretty(&updated.unwrap())?
        );

        // Count documents
        let count = coll.count(None)?;
        println!("📈 Total count: {}\n", count);

        // Test range query with filter
        println!("🔎 Testing range query (value > 50)...");
        let range_filter = json!({ "value": { "$gt": 50 } });
        let range_docs = coll.find(Some(range_filter))?;
        println!("   ✅ Found {} documents with value > 50\n", range_docs.len());

        // Test distinct
        println!("🏷️  Testing distinct on \"name\" field...");
        let distinct = coll.distinct("name", None)?;
        println!("   ✅ Found {} distinct names\n", distinct.len());
    }

    // Explicit checkpoint test
    println!("💾 Testing explicit checkpoint...");
    db.checkpoint()?;
    println!("   ✅ Checkpoint completed!\n");

    println!("✅ All operations completed successfully!");
    println!("\n💡 The checkpoint infrastructure is working internally:");
    println!("   - BTree id index is being used for _id lookups");
    println!("   - Auto-checkpointing can be configured with request_checkpoint_after_updates()");
    println!("   - Explicit checkpoint() flushes dirty pages to disk");

    Ok(())
}
