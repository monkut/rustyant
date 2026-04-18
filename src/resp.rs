use bytes::{Bytes, BytesMut};
use bytes_utils::Str;
use redis_protocol::resp2::{decode::decode_bytes_mut, encode::extend_encode, types::BytesFrame};

use crate::error::RustyAntError;

#[derive(Debug, Clone)]
pub enum RespReply {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Vec<Self>),
    Nil,
}

impl RespReply {
    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }

    pub fn err<S: Into<String>>(msg: S) -> Self {
        Self::Error(msg.into())
    }

    pub fn to_frame(&self) -> BytesFrame {
        match self {
            Self::SimpleString(s) => BytesFrame::SimpleString(Bytes::from(s.clone().into_bytes())),
            Self::Error(s) => BytesFrame::Error(Str::from(s.clone())),
            Self::Integer(i) => BytesFrame::Integer(*i),
            Self::BulkString(Some(b)) => BytesFrame::BulkString(b.clone()),
            Self::BulkString(None) | Self::Nil => BytesFrame::Null,
            Self::Array(items) => BytesFrame::Array(items.iter().map(Self::to_frame).collect()),
        }
    }

    pub fn encode(&self) -> Result<Bytes, RustyAntError> {
        let frame = self.to_frame();
        let mut buf = BytesMut::new();
        extend_encode(&mut buf, &frame).map_err(|e| RustyAntError::RespParse(format!("encode: {e}")))?;
        Ok(buf.freeze())
    }
}

pub fn parse_command(body: &[u8]) -> Result<Vec<Bytes>, RustyAntError> {
    let mut buf = BytesMut::from(body);
    let decoded = decode_bytes_mut(&mut buf)
        .map_err(|e| RustyAntError::RespParse(format!("decode: {e}")))?
        .ok_or_else(|| RustyAntError::RespParse("incomplete frame".into()))?;
    let frame = decoded.0;

    match frame {
        BytesFrame::Array(items) => items.into_iter().map(frame_to_bytes).collect::<Result<Vec<_>, _>>(),
        BytesFrame::BulkString(b) => Ok(vec![b]),
        _ => Err(RustyAntError::RespParse("expected array of bulk strings".into())),
    }
}

fn frame_to_bytes(frame: BytesFrame) -> Result<Bytes, RustyAntError> {
    match frame {
        BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => Ok(b),
        _ => Err(RustyAntError::RespParse("argument must be bulk string".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_set_command() {
        let wire = b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let argv = parse_command(wire).expect("parse ok");
        assert_eq!(argv.len(), 3);
        assert_eq!(&argv[0][..], b"SET");
        assert_eq!(&argv[1][..], b"hello");
        assert_eq!(&argv[2][..], b"world");
    }

    #[test]
    fn rejects_incomplete_frame() {
        let wire = b"*3\r\n$3\r\nSET\r\n";
        let err = parse_command(wire).expect_err("should fail");
        assert!(matches!(err, RustyAntError::RespParse(_)));
    }

    #[test]
    fn encodes_simple_string_ok() {
        let reply = RespReply::ok();
        let encoded = reply.encode().expect("encode ok");
        assert_eq!(&encoded[..], b"+OK\r\n");
    }

    #[test]
    fn encodes_integer() {
        let reply = RespReply::Integer(42);
        let encoded = reply.encode().expect("encode ok");
        assert_eq!(&encoded[..], b":42\r\n");
    }

    #[test]
    fn encodes_bulk_string() {
        let reply = RespReply::BulkString(Some(Bytes::from_static(b"hello")));
        let encoded = reply.encode().expect("encode ok");
        assert_eq!(&encoded[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn encodes_nil() {
        let reply = RespReply::Nil;
        let encoded = reply.encode().expect("encode ok");
        assert_eq!(&encoded[..], b"$-1\r\n");
    }

    #[test]
    fn encodes_error() {
        let reply = RespReply::err("WRONGTYPE mismatch");
        let encoded = reply.encode().expect("encode ok");
        assert_eq!(&encoded[..], b"-WRONGTYPE mismatch\r\n");
    }

    #[test]
    fn encodes_array_of_bulks() {
        let reply = RespReply::Array(vec![
            RespReply::BulkString(Some(Bytes::from_static(b"a"))),
            RespReply::BulkString(Some(Bytes::from_static(b"bb"))),
        ]);
        let encoded = reply.encode().expect("encode ok");
        assert_eq!(&encoded[..], b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n");
    }
}
