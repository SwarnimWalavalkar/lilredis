use std::io::prelude::*;
use std::{collections::HashMap, path::PathBuf};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let key = &args[1];
    let value = &args[2];
    
    let mut database = Database::new().expect("Corrupt database");
    database.insert(key.to_string(), value.to_string());
}

struct Database {
    map: HashMap<String, String>,
}

impl Database {
    fn new() -> Result<Database, std::io::Error> {
        let mut map: HashMap<String, String> = HashMap::new();
        
        let mut contents = String::new();
        let path = PathBuf::from("db");
        if path.exists() {
            let file = std::fs::File::open(path)?;
            let mut buf_reader = std::io::BufReader::new(file);
            buf_reader.read_to_string(&mut contents)?;
        } else {
            std::fs::File::create("db")?;
        }

        for line in contents.lines() {
            let (key, value) = line.split_once("\t").expect("Corrupt database");
            map.insert(key.to_string(), value.to_string());
        }
        Ok(Database { map })
    }

    fn insert(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = do_flush(self);
    }
}

fn do_flush(database: &Database) -> std::io::Result<()> {
    let mut contents = String::new();
    for (key, value) in &database.map {
        contents.push_str(key);
        contents.push('\t');
        contents.push_str(value);
        contents.push('\n');
    }
    std::fs::write("db", contents).expect("Unable to create db");
    Ok(())
}