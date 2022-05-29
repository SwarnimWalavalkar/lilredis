use core::panic;
use std::{fs::{File, OpenOptions}, collections::HashMap, path::Path, io::{Result, BufReader, SeekFrom, Read, Seek, Write}};

use byteorder::{ReadBytesExt, LittleEndian, WriteBytesExt};
use crc::crc32;
use serde_derive::{Deserialize, Serialize};

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
  pub key: ByteString,
  pub value: ByteString,
}

#[derive(Debug)]
pub struct LilRedis {
  file: File,
  pub index: HashMap<ByteString, u64>,
}

impl LilRedis {
  pub fn seek_to_end(&mut self) -> Result<u64> {
    self.file.seek(SeekFrom::End(0))
  }

  pub fn open(path: &Path) -> Result<Self> {
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .append(true)
      .open(path)?;
    
    let index = HashMap::new();

    Ok(LilRedis {file, index})
  }

  pub fn load(&mut self) -> Result<()> {
    let mut file = BufReader::new(&mut self.file);

    loop {
      let current_position = file.seek(SeekFrom::Current(0))?;

      let maybe_kv = LilRedis::process_record(&mut file);
      let kv = match maybe_kv {
        Ok(kv) => kv,
        Err(err) => {
          match err.kind() {
            std::io::ErrorKind::UnexpectedEof => {
              break;
            }
            _ => return Err(err),
          }
        }
      };

      self.index.insert(kv.key, current_position);
    }

    Ok(())
  }

  fn process_record<R: Read>(                    
    file: &mut R
  ) -> Result<KeyValuePair> {
    let saved_checksum =
      file.read_u32::<LittleEndian>()?;
    let key_len =
      file.read_u32::<LittleEndian>()?;
    let val_len =
      file.read_u32::<LittleEndian>()?;
    let data_len = key_len + val_len;

    let mut data = ByteString::with_capacity(data_len as usize);

    {
      file.by_ref()
        .take(data_len as u64)
        .read_to_end(&mut data)?;
    }
    debug_assert_eq!(data.len(), data_len as usize);

    let checksum = crc32::checksum_ieee(&data);
    if checksum != saved_checksum {
      panic!(
        "data corruption encountered ({:08x} != {:08x})",
        checksum, saved_checksum
      );
    }

    let value = data.split_off(key_len as usize);
    let key = data;

    Ok(KeyValuePair { key, value })
  }

  pub fn get(&mut self, key: &ByteStr) -> Result<Option<ByteString>> {
    let position = match self.index.get(key) {
      None => return Ok(None),
      Some(position) => *position,
    };

    let kv = self.get_at(position)?;

    Ok(Some(kv.value))
  }

  fn get_at(&mut self, position: u64) -> Result<KeyValuePair> {
    let mut file = BufReader::new(&mut self.file);
    file.seek(SeekFrom::Start(position))?;
    let kv = LilRedis::process_record(&mut file)?;

    Ok(kv)
  }

  pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> Result<()> {
    let position = self.insert_but_ignore_index(key, value)?;

    self.index.insert(key.to_vec(), position);
    Ok(())
  }

  fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> Result<u64> {
    let mut file = std::io::BufWriter::new(&mut self.file);

    let key_len = key.len();
    let val_len = value.len();
    let mut tmp = ByteString::with_capacity(key_len + val_len);

    for byte in key {
      tmp.push(*byte);
    }

    for byte in value {
      tmp.push(*byte);
    }

    let checksum = crc32::checksum_ieee(&tmp);

    let next_byte = SeekFrom::End(0);
    let current_position = file.seek(SeekFrom::Current(0))?;
    file.seek(next_byte)?;
    file.write_u32::<LittleEndian>(checksum)?;
    file.write_u32::<LittleEndian>(key_len as u32)?;
    file.write_u32::<LittleEndian>(val_len as u32)?;
    file.write_all(&tmp)?;

    Ok(current_position)
  }

  #[inline]
  pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> Result<()> {
    self.insert(key, value)
  }

  #[inline]
  pub fn delete(&mut self, key: &ByteStr) -> Result<()> {
    self.insert(key, b"")
  }
}