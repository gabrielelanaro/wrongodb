use crate::commands::Command;
use crate::{WrongoDB, WrongoDBError};
use bson::{doc, Bson, Document};

/// Handles: hello, isMaster, ismaster
pub struct HelloCommand;

impl Command for HelloCommand {
    fn names(&self) -> &[&str] {
        &["hello", "ismaster", "isMaster"]
    }

    fn execute(&self, _doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "isMaster": Bson::Boolean(true),
            "isWritablePrimary": Bson::Boolean(true),
            "helloOk": Bson::Boolean(true),
            "maxBsonObjectSize": Bson::Int32(16777216),
            "maxMessageSizeBytes": Bson::Int32(48000000),
            "maxWriteBatchSize": Bson::Int32(100000),
            "localTime": Bson::DateTime(bson::DateTime::now()),
            "minWireVersion": Bson::Int32(0),
            "maxWireVersion": Bson::Int32(13),
            "readOnly": Bson::Boolean(false),
            "logicalSessionTimeoutMinutes": Bson::Int32(30),
            "connectionId": Bson::Int32(1),
            "compression": Bson::Array(vec![]),
        })
    }
}

/// Handles: ping
pub struct PingCommand;

impl Command for PingCommand {
    fn names(&self) -> &[&str] {
        &["ping"]
    }

    fn execute(&self, _doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        Ok(doc! { "ok": Bson::Double(1.0) })
    }
}

/// Handles: buildInfo, buildinfo
pub struct BuildInfoCommand;

impl Command for BuildInfoCommand {
    fn names(&self) -> &[&str] {
        &["buildInfo", "buildinfo"]
    }

    fn execute(&self, _doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "version": "0.0.1-wrongodb",
            "gitVersion": "unknown",
            "modules": Bson::Array(vec![]),
            "sysInfo": "WrongoDB",
            "versionArray": Bson::Array(vec![Bson::Int32(0), Bson::Int32(0), Bson::Int32(1)]),
            "javascriptEngine": "none",
            "openssl": Bson::Document(Document::new()),
            "buildEnvironment": Bson::Document(Document::new()),
            "bits": Bson::Int32(64),
            "debug": Bson::Boolean(false),
        })
    }
}

/// Handles: serverStatus
pub struct ServerStatusCommand;

impl Command for ServerStatusCommand {
    fn names(&self) -> &[&str] {
        &["serverStatus"]
    }

    fn execute(&self, _doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "host": "localhost",
            "version": "0.0.1-wrongodb",
            "process": "wrongodb",
            "pid": Bson::Int32(1),
            "uptime": Bson::Double(0.0),
            "connections": {
                "current": Bson::Int32(1),
                "available": Bson::Int32(1000),
            },
        })
    }
}

/// Handles: connectionStatus
pub struct ConnectionStatusCommand;

impl Command for ConnectionStatusCommand {
    fn names(&self) -> &[&str] {
        &["connectionStatus"]
    }

    fn execute(&self, _doc: &Document, _db: &WrongoDB) -> Result<Document, WrongoDBError> {
        Ok(doc! {
            "ok": Bson::Double(1.0),
            "authInfo": {
                "authenticatedUsers": Bson::Array(vec![]),
                "authenticatedUserRoles": Bson::Array(vec![]),
            },
        })
    }
}
