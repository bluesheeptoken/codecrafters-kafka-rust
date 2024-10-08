use super::model;
use bytes::Buf;
use std::error::Error;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, PartialEq)]
pub enum Request {
    ApiVersions(ApiVersions),
    Fetch(Fetch),
}

#[derive(Debug, PartialEq)]
pub struct ApiVersions {
    pub header: RequestHeader,
}

#[derive(Debug, PartialEq)]
pub struct Fetch {
    pub header: RequestHeader,
    pub session_id: i32,
    pub topics: Vec<model::Topic>,
}

#[derive(Debug, PartialEq)]
pub struct RequestHeader {
    pub request_api_key: model::ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

pub trait HasRequestHeader {
    fn header(&self) -> &RequestHeader;
}

impl HasRequestHeader for Request {
    fn header(&self) -> &RequestHeader {
        match self {
            Request::ApiVersions(api_versions_request) => &api_versions_request.header,
            Request::Fetch(fetch_request) => &fetch_request.header,
        }
    }
}

impl Request {
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

        dbg!(&request_header);

        Ok(match request_header.request_api_key {
            model::ApiKey::Versions => {
                Request::ApiVersions(Request::parse_api_versions(request_header, &mut request))
            }
            model::ApiKey::Fetch => {
                Request::Fetch(Request::parse_fetch(request_header, &mut request))
            }
        })
    }

    fn parse_fetch(header: RequestHeader, request: &mut Cursor<Vec<u8>>) -> Fetch {
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
            let topic = Request::parse_topic(request);
            topics.push(topic);
        }
        let _forgotten_topics_data_count = request.get_u8().saturating_sub(1);
        let _rack_id_len = request.get_u8() - 1;
        request.advance(1); // TAG_BUFFER

        let request = Fetch {
            header,
            session_id: session_id,
            topics: topics,
        };
        dbg!(&request);
        request
    }

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

    fn parse_api_versions(header: RequestHeader, _buffer: &mut Cursor<Vec<u8>>) -> ApiVersions {
        ApiVersions { header: header }
    }

    // TODO: API key should not be matched at all
    pub fn is_request_api_version_header_valid(&self) -> bool {
        match self {
            Request::ApiVersions(api_versions_request) => Self::is_request_api_version_valid(
                api_versions_request.header.request_api_key,
                api_versions_request.header.request_api_version,
            ),
            Request::Fetch(fetch_request) => Self::is_request_api_version_valid(
                fetch_request.header.request_api_key,
                fetch_request.header.request_api_version,
            ),
        }
    }

    fn is_request_api_version_valid(key: model::ApiKey, version: i16) -> bool {
        match key {
            model::ApiKey::Versions => (model::ApiKeyVariant::Versions)
                .versions()
                .is_version_valid(version),
            model::ApiKey::Fetch => (model::ApiKeyVariant::Fetch)
                .versions()
                .is_version_valid(version),
        }
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
        let expected_request = Request::ApiVersions(super::ApiVersions {
            header: RequestHeader {
                request_api_key: request_api_key,
                request_api_version: request_api_version,
                correlation_id: correlation_id,
            },
        });

        assert_eq!(request, expected_request);
    }

    #[tokio::test]
    async fn test_parse_fetch_request() {
        use bytes::BufMut;
        let (mut client, mut server) = tokio::io::duplex(512);
        let mut request_data = vec![];

        let request_body_length = 89;
        let request_api_key: model::ApiKey = model::ApiKey::Fetch;
        let request_api_version: i16 = 18;
        let correlation_id: i32 = 42;

        request_data.put_i32(request_body_length as i32);
        request_data.put_i16(request_api_key as i16);
        request_data.put_i16(request_api_version);
        request_data.put_i32(correlation_id);

        let client_id_len = 2;
        request_data.put_u16(client_id_len);
        request_data.put_bytes(0, client_id_len as usize);
        request_data.put_u8(0);

        let max_wait_ms = 1;
        let min_bytes = 2;
        let max_bytes = 3;
        let isolation_level = 4;
        let session_id = 5;
        let session_epoch = 6;
        request_data.put_i32(max_wait_ms);
        request_data.put_i32(min_bytes);
        request_data.put_i32(max_bytes);
        request_data.put_i8(isolation_level);
        request_data.put_i32(session_id);
        request_data.put_i32(session_epoch);

        let topic_count = 1;
        request_data.put_u8(topic_count + 1);

        for _ in 0..topic_count {
            let uuid = 7;
            request_data.put_u128(uuid);
            let partition_count = 1;
            request_data.put_u8(partition_count + 1);
            for _ in 0..partition_count {
                let partition = 8;
                let current_leader_epoch = 9;
                let fetch_offset = 10;
                let last_fetched_epoch = 11;
                let log_start_offset = 12;
                let partition_max_bytes = 13;
                request_data.put_u32(partition);
                request_data.put_u32(current_leader_epoch);
                request_data.put_u64(fetch_offset);
                request_data.put_u32(last_fetched_epoch);
                request_data.put_u64(log_start_offset);
                request_data.put_u32(partition_max_bytes);
                request_data.put_u8(0); // TAG_BUFFER
            }
            request_data.put_u8(0); // TAG_BUFFER
        }
        let forgotten_topics_data_count = 1;
        request_data.put_u8(forgotten_topics_data_count - 1);

        let rack_id_len = 0;
        request_data.put_u8(rack_id_len + 1);

        request_data.put_u8(0); // TAG_BUFFER

        client.write_all(&request_data).await.unwrap();
        client.shutdown().await.unwrap();

        let request = Request::parse_request(&mut server).await.unwrap();
        let expected_request = Request::Fetch(super::Fetch {
            header: RequestHeader {
                request_api_key: request_api_key,
                request_api_version: request_api_version,
                correlation_id: correlation_id,
            },
            session_id: 5,
            topics: vec![model::Topic { id: 7 }],
        });

        assert_eq!(request, expected_request);
    }
}
