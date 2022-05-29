use core::panic;

use liblilredis::LilRedis;

const USAGE: &str = "
Usage:
    lilredis FILE get KEY
    lilredis FILE delete KEY
    lilredis FILE insert KEY VALUE
    lilredis FILE update KEY VALUE
";

fn main() {
  let args: Vec<String> = std::env::args().collect();
  let file_name = args.get(1).expect(&USAGE);
  let command = args.get(2).expect(&USAGE).as_ref();
  let key = args.get(3).expect(&USAGE).as_ref();
  let maybe_value = args.get(4);

  let path = std::path::Path::new(&file_name);
  let mut store = LilRedis::open(path).expect("error opening file");
  store.load().expect("error loading data");

  match command {
    "get" => match store.get(key).unwrap() {
      None => eprint!("{:?} not found", key),
      Some(value) => {
        let val_str = match std::str::from_utf8(&value) {
          Ok(v) => v,
          Err(e) => panic!("Invalid in key: {}", e)
        };

        println!("{:?}", val_str)

      },
    },

    "delete" => store.delete(key).unwrap(),

    "insert" => {
      let value = maybe_value.expect(&USAGE).as_ref();
      store.insert(key, value).unwrap()
    }

    "update" => {
      let value = maybe_value.expect(&USAGE).as_ref();
      store.update(key, value).unwrap()
    }

    _ => eprint!("{}", &USAGE),
  }
  

}