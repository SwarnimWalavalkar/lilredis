# A very naive Redis clone

A simple in-memory key-value datastore written in Rust,

## Building

```bash
cargo build -r --target-dir ./build
cd build/release
```

## Usage

```
./lilredis FILE get KEY
./lilredis FILE delete KEY
./lilredis FILE insert KEY VALUE
./lilredis FILE update KEY VALUE
```

## Usage Examples

Inserting (test.db will be created if it doesnt already exist)

```bash
$ ./lilredis test.db insert hello world
```

```bash
$ ./lilredis test.db insert balance 4204
"4222"
```

Fetching a value with a key

```bash
$ ./lilredis test.db get hello
"world"
```

Updating a value with a key

```bash
$ ./lilredis test.db update balance 4242
"4242"
```

Deleting a value with a key

```bash
$ ./lilredis test.db delete hello
"world"
```
