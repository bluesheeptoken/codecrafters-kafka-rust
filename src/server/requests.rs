use super::model;
use bytes::Buf;
use std::error::Error;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct Request {
    pub header: RequestHeader,
    pub body: RequestBody,
}
#[derive(Debug, PartialEq)]
pub struct RequestHeader {
    pub request_api_key: model::ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl Request {
    // TODO: Weird conversion here
    pub fn is_request_api_version_valid(&self, version: i16) -> bool {
        match self.header.request_api_key {
            model::ApiKey::Versions => (model::ApiKeyVariant::Versions)
                .versions()
                .is_version_valid(version),
            model::ApiKey::Fetch => (model::ApiKeyVariant::Fetch)
                .versions()
                .is_version_valid(version),
        }
    }

    pub async fn parse_request<R>(stream: &mut R) -> Result<Request, Box<dyn Error>>
    where
        R: AsyncRead + Unpin,
    {
        let mut len = [0; 4];
        stream.read_exact(&mut len).await?;
        let len = i32::from_be_bytes(len) as usize;

        let mut request = vec![0; len];
        stream.read_exact(&mut request).await?;

        let mut request = Cursor::new(request);

        let request_header = RequestHeader {
            request_api_key: model::ApiKey::parse(request.get_i16())?,
            request_api_version: request.get_i16(),
            correlation_id: request.get_i32(),
        };

        let body: RequestBody = match request_header.request_api_key {
            model::ApiKey::Versions => {
                RequestBody::ApiVersions(ApiVersionsRequest::parse(&mut request))
            }
            model::ApiKey::Fetch => RequestBody::Fetch(FetchRequest::parse(&mut request)),
        };

        Ok(Request {
            header: request_header,
            body: body,
        })
    }
}

pub trait RequestParser: Send {
    fn parse(buffer: &mut Cursor<Vec<u8>>) -> Self
    where
        Self: Sized;
}

pub enum RequestBody {
    ApiVersions(ApiVersionsRequest),
    Fetch(FetchRequest),
}

#[derive(Debug, PartialEq)]
pub struct ApiVersionsRequest {}

impl RequestParser for ApiVersionsRequest {
    fn parse(_buffer: &mut Cursor<Vec<u8>>) -> Self {
        ApiVersionsRequest {}
    }
}

#[derive(Debug, PartialEq)]
pub struct FetchRequest {
    pub session_id: i32,
    pub topics: Vec<model::Topic>,
}

impl FetchRequest {
    fn parse_topic(request: &mut Cursor<Vec<u8>>) -> model::Topic {
        let uuid = request.get_u128();
        let partition_count = request.get_u8().saturating_sub(1); // Might be buggy, use varint
        for _ in 0..partition_count {
            let _partition = request.get_u32();
            let _current_leader_epoch = request.get_u32();
            let _fetch_offset = request.get_u64();
            let _last_fetched_epoch = request.get_u32();
            let _log_start_offset = request.get_u64();
            let _partition_max_bytes = request.get_u32();
            request.advance(1); // TAG_BUFFER
        }
        request.advance(1); // TAG_BUFFER
        model::Topic { id: uuid }
    }
}

impl RequestParser for FetchRequest {
    fn parse(request: &mut Cursor<Vec<u8>>) -> Self {
        let client_id_len = request.get_u16() as usize;
        request.advance(client_id_len);
        request.advance(1); // TAG_BUFFER

        let _max_wait_ms = request.get_i32();
        let _min_bytes = request.get_i32();
        let _max_bytes = request.get_i32();
        let _isolation_level = request.get_i8();
        let session_id = request.get_i32();
        let _session_epoch = request.get_i32();

        let topic_length = request.get_u8().saturating_sub(1); // might be buggy, should read varint instead
        let mut topics: Vec<model::Topic> = Vec::with_capacity(topic_length as usize);
        for _ in 0..topic_length {
            let topic = FetchRequest::parse_topic(request);
            topics.push(topic);
        }
        let _forgotten_topics_data_count = request.get_u8().saturating_sub(1);
        let _rack_id_len = request.get_u8() - 1;
        request.advance(1); // TAG_BUFFER

        let request = FetchRequest {
            session_id: session_id,
            topics: topics,
        };
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use tokio::io::AsyncWriteExt;
    #[tokio::test]
    async fn test_parse_request_api_versions_request() {
        let (mut client, mut server) = tokio::io::duplex(512);

        let request_body_length = 8;
        let request_api_key: model::ApiKey = model::ApiKey::Versions;
        let request_api_version: i16 = 2;
        let correlation_id: i32 = 42;

        let mut request_data = vec![];
        request_data.put_i32(request_body_length as i32);
        request_data.put_i16(request_api_key as i16);
        request_data.put_i16(request_api_version);
        request_data.put_i32(correlation_id);

        client.write_all(&request_data).await.unwrap();
        client.shutdown().await.unwrap();

        let request = Request::parse_request(&mut server).await.unwrap();
        let expected_header = RequestHeader {
            request_api_key: request_api_key,
            request_api_version: request_api_version,
            correlation_id: correlation_id,
        };

        assert_eq!(request.header, expected_header);
        // TODO: check body !
    }

    #[test]
    fn test_parse_fetch_request() {
        use bytes::BufMut;
        let mut buffer: Vec<u8> = vec![];

        let client_id_len = 2;
        buffer.put_u16(client_id_len);
        buffer.put_bytes(0, client_id_len as usize);
        buffer.put_u8(0);

        let max_wait_ms = 1;
        let min_bytes = 2;
        let max_bytes = 3;
        let isolation_level = 4;
        let session_id = 5;
        let session_epoch = 6;
        buffer.put_i32(max_wait_ms);
        buffer.put_i32(min_bytes);
        buffer.put_i32(max_bytes);
        buffer.put_i8(isolation_level);
        buffer.put_i32(session_id);
        buffer.put_i32(session_epoch);

        let topic_count = 1;
        buffer.put_u8(topic_count + 1);

        for _ in 0..topic_count {
            let uuid = 7;
            buffer.put_u128(uuid);
            let partition_count = 1;
            buffer.put_u8(partition_count + 1);
            for _ in 0..partition_count {
                let partition = 8;
                let current_leader_epoch = 9;
                let fetch_offset = 10;
                let last_fetched_epoch = 11;
                let log_start_offset = 12;
                let partition_max_bytes = 13;
                buffer.put_u32(partition);
                buffer.put_u32(current_leader_epoch);
                buffer.put_u64(fetch_offset);
                buffer.put_u32(last_fetched_epoch);
                buffer.put_u64(log_start_offset);
                buffer.put_u32(partition_max_bytes);
                buffer.put_u8(0); // TAG_BUFFER
            }
            buffer.put_u8(0); // TAG_BUFFER
        }
        let forgotten_topics_data_count = 1;
        buffer.put_u8(forgotten_topics_data_count - 1);

        let rack_id_len = 0;
        buffer.put_u8(rack_id_len + 1);

        buffer.put_u8(0); // TAG_BUFFER

        let request = FetchRequest::parse(&mut Cursor::new(buffer));
        let expected_body = FetchRequest {
            session_id: 5,
            topics: vec![model::Topic { id: 7 }],
        };

        assert_eq!(request, expected_body);
    }
}
