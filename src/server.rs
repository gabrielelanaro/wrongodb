use std::collections::HashMap;
use std::io::{self, Cursor};
use std::sync::Arc;

use bson::{doc, Bson, Document};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::commands::handlers::crud::{bson_to_value, value_to_bson};
use crate::{CommandRegistry, WrongoDB, WrongoDBError};

const OP_MSG: i32 = 2013;
const OP_QUERY: i32 = 2004;
const OP_REPLY: i32 = 1;

#[derive(Debug)]
struct MsgHeader {
    message_length: i32,
    request_id: i32,
    response_to: i32,
    op_code: i32,
}

impl MsgHeader {
    fn read_from_slice(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(buf);
        Ok(Self {
            message_length: ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?,
            request_id: ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?,
            response_to: ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?,
            op_code: ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?,
        })
    }

    fn write_to_vec(&self, buf: &mut Vec<u8>) -> io::Result<()> {
        WriteBytesExt::write_i32::<LittleEndian>(buf, self.message_length)?;
        WriteBytesExt::write_i32::<LittleEndian>(buf, self.request_id)?;
        WriteBytesExt::write_i32::<LittleEndian>(buf, self.response_to)?;
        WriteBytesExt::write_i32::<LittleEndian>(buf, self.op_code)?;
        Ok(())
    }
}

pub async fn start_server(
    addr: &str,
    db: Arc<Mutex<WrongoDB>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    let registry = Arc::new(CommandRegistry::new());
    println!("Server listening on {}", addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let db_clone = Arc::clone(&db);
        let registry_clone = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, db_clone, registry_clone).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    db: Arc<Mutex<WrongoDB>>,
    registry: Arc<CommandRegistry>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut header_buf = [0u8; 16];
        if let Err(e) = socket.read_exact(&mut header_buf).await {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(e.into());
        }
        let header = MsgHeader::read_from_slice(&header_buf)?;

        let body_len = header.message_length - 16;
        if body_len < 0 {
            return Err("invalid message length".into());
        }
        let mut body_buf = vec![0u8; body_len as usize];
        let mut read_bytes = 0usize;
        while read_bytes < body_buf.len() {
            match socket.read(&mut body_buf[read_bytes..]).await {
                Ok(0) if read_bytes == 0 => return Ok(()),
                Ok(0) => {
                    return Err(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete body").into(),
                    )
                }
                Ok(n) => read_bytes += n,
                Err(e) => return Err(e.into()),
            }
        }

        match header.op_code {
            OP_MSG => {
                let response = handle_op_msg(&body_buf, &db, &registry).await?;
                send_op_msg_response(&mut socket, header.request_id, &response).await?;
            }
            OP_QUERY => {
                let response = handle_op_query(&body_buf, &db, &registry).await?;
                send_reply(&mut socket, header.request_id, &response).await?;
            }
            _ => continue,
        }
    }
}

async fn handle_op_msg(
    body_buf: &[u8],
    db: &Arc<Mutex<WrongoDB>>,
    registry: &CommandRegistry,
) -> Result<Document, WrongoDBError> {
    let mut cursor = Cursor::new(body_buf);
    let _flag_bits = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;
    let mut command_doc: Option<Document> = None;
    let mut doc_sequences: HashMap<String, Vec<Document>> = HashMap::new();

    while (cursor.position() as usize) < body_buf.len() {
        let kind = ReadBytesExt::read_u8(&mut cursor)?;
        if kind == 0 {
            match bson::from_reader::<_, Document>(&mut cursor) {
                Ok(doc) => command_doc = Some(doc),
                Err(e) => return Err(WrongoDBError::BsonDe(e)),
            }
        } else if kind == 1 {
            let size = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
            let section_start = cursor.position();
            let mut ident_bytes = Vec::new();
            loop {
                let b = ReadBytesExt::read_u8(&mut cursor)?;
                if b == 0 {
                    break;
                }
                ident_bytes.push(b);
            }
            let identifier = String::from_utf8(ident_bytes).unwrap_or_default();
            let mut docs = Vec::new();
            while (cursor.position() - section_start) < (size as u64 - 4) {
                match bson::from_reader::<_, Document>(&mut cursor) {
                    Ok(doc) => docs.push(doc),
                    Err(e) => return Err(WrongoDBError::BsonDe(e)),
                }
            }
            doc_sequences.insert(identifier, docs);
        } else {
            break;
        }
    }

    if let Some(mut doc) = command_doc {
        if let Some(seq_docs) = doc_sequences.get("documents") {
            if doc.get("documents").is_none() {
                let docs_bson = seq_docs.iter().cloned().map(Bson::Document).collect();
                doc.insert("documents", Bson::Array(docs_bson));
            }
        }
        let mut db_lock = db.lock().await;
        return registry.execute(&doc, &mut db_lock);
    }

    Ok(doc! { "ok": Bson::Double(0.0), "errmsg": "No command document" })
}

async fn handle_op_query(
    body_buf: &[u8],
    db: &Arc<Mutex<WrongoDB>>,
    registry: &CommandRegistry,
) -> Result<Document, WrongoDBError> {
    let mut cursor = Cursor::new(body_buf);
    let _flags = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

    let mut coll_name = Vec::new();
    loop {
        let byte = ReadBytesExt::read_u8(&mut cursor)?;
        if byte == 0 {
            break;
        }
        coll_name.push(byte);
    }
    let full_coll_name = String::from_utf8(coll_name).unwrap_or_default();

    let _number_to_skip = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
    let _number_to_return = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

    let query_len = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
    let mut query_buf = vec![0u8; query_len as usize];
    query_buf[..4].copy_from_slice(&query_len.to_le_bytes());
    std::io::Read::read_exact(&mut cursor, &mut query_buf[4..])?;

    let query_doc: Document = bson::from_slice(&query_buf).map_err(WrongoDBError::BsonDe)?;

    if full_coll_name == "admin.$cmd" {
        let mut db_lock = db.lock().await;
        return registry.execute(&query_doc, &mut db_lock);
    }

    let filter_json = bson_to_value(&query_doc);
    let mut db_lock = db.lock().await;
    let results = db_lock.find(Some(filter_json))?;
    let results_bson: Vec<Bson> = results
        .into_iter()
        .map(|d| Bson::Document(value_to_bson(&Value::Object(d))))
        .collect();

    Ok(doc! {
        "ok": Bson::Double(1.0),
        "cursor": {
            "id": Bson::Int64(0),
            "ns": full_coll_name,
            "firstBatch": Bson::Array(results_bson),
        }
    })
}

async fn send_reply(
    socket: &mut TcpStream,
    request_id: i32,
    doc: &Document,
) -> Result<(), Box<dyn std::error::Error>> {
    let doc_bytes = bson::to_vec(doc)?;
    let message_length = 16 + 4 + 8 + 4 + 4 + doc_bytes.len() as i32;

    let mut buf = Vec::new();
    MsgHeader {
        message_length,
        request_id: request_id + 1,
        response_to: request_id,
        op_code: OP_REPLY,
    }
    .write_to_vec(&mut buf)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut buf, 0)?;
    WriteBytesExt::write_i64::<LittleEndian>(&mut buf, 0)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut buf, 0)?;
    WriteBytesExt::write_i32::<LittleEndian>(&mut buf, 1)?;
    buf.extend_from_slice(&doc_bytes);

    socket.write_all(&buf).await?;
    Ok(())
}

async fn send_op_msg_response(
    socket: &mut TcpStream,
    request_id: i32,
    doc: &Document,
) -> Result<(), Box<dyn std::error::Error>> {
    let doc_bytes = bson::to_vec(doc)?;
    let message_length = 16 + 4 + 1 + doc_bytes.len() as i32;

    let mut buf = Vec::new();
    MsgHeader {
        message_length,
        request_id: request_id + 1,
        response_to: request_id,
        op_code: OP_MSG,
    }
    .write_to_vec(&mut buf)?;
    WriteBytesExt::write_u32::<LittleEndian>(&mut buf, 0)?;
    WriteBytesExt::write_u8(&mut buf, 0)?;
    buf.extend_from_slice(&doc_bytes);

    socket.write_all(&buf).await?;
    Ok(())
}
