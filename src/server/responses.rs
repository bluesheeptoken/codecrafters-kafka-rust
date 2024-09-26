use super::model::ApiKeyVariant;
use super::model::WireSerialization;
use super::requests::{ApiVersionsRequest, FetchRequest};

pub mod api_versions {
    use bytes::BufMut;

    use crate::server::ErrorCode;
    pub struct ApiVersionV4Response {
        pub api_key_versions: Vec<super::ApiKeyVariant>,
        pub throttle_time_in_ms: i32,
    }

    impl ApiVersionV4Response {
        pub fn process_request(
            _request: &super::ApiVersionsRequest,
            error_code: ErrorCode,
        ) -> Result<ApiVersionV4Response, ErrorCode> {
            if error_code != ErrorCode::Ok {
                Err(error_code)
            } else {
                Ok(ApiVersionV4Response {
                    api_key_versions: vec![
                        super::ApiKeyVariant::Fetch,
                        super::ApiKeyVariant::Versions,
                    ],
                    throttle_time_in_ms: 0,
                })
            }
        }
    }

    impl super::WireSerialization for Result<ApiVersionV4Response, ErrorCode> {
        // https://kafka.apache.org/protocol#The_Messages_ApiVersions
        fn to_wire_format(&self, buffer: &mut Vec<u8>) -> () {
            match self {
                Ok(response) => {
                    buffer.put_i16(ErrorCode::Ok as i16);

                    let number_of_tagged_fields = response.api_key_versions.len() + 1; // verint offset by 1 to reserve 0 for null values
                    buffer.put_i8(number_of_tagged_fields as i8); // might be buggy for number > 7 as this is encoded as variable length encoding TODO: fix

                    for api_key_version in &response.api_key_versions {
                        api_key_version.to_wire_format(buffer);
                    }
                    buffer.put_i32(response.throttle_time_in_ms); // throttle time in ms
                    buffer.put_i8(0); // no tagged fields, null marker
                }
                Err(error_code) => buffer.put_i16(*error_code as i16),
            }
        }
    }
}

pub mod fetch {
    use bytes::BufMut;
    #[derive(Debug)]
    pub struct FetchV16Response {
        throttle_time_in_ms: i32,
        session_id: i32,
        topics: Vec<FetchTopicResponse>,
    }

    impl FetchV16Response {
        pub fn process_request(request: &super::FetchRequest) -> FetchV16Response {
            FetchV16Response {
                throttle_time_in_ms: 0,
                session_id: 0,
                topics: request
                    .topics
                    .iter()
                    .map(|topic| FetchTopicResponse { topic_id: topic.id })
                    .collect(),
            }
        }
    }

    #[derive(Debug)]
    pub struct FetchTopicResponse {
        topic_id: u128,
    }

    impl super::WireSerialization for FetchV16Response {
        // https://kafka.apache.org/protocol.html#The_Messages_Fetch
        fn to_wire_format(&self, buffer: &mut Vec<u8>) -> () {
            buffer.put_i8(0); // no tagged fields null marker // TODO: this should not be here?
            buffer.put_i32(self.throttle_time_in_ms);
            buffer.put_i16(0); // error code, 0 for now
            buffer.put_i32(self.session_id);
            buffer.put_u8(self.topics.len() as u8 + 1);
            for topic in &self.topics {
                buffer.put_u128(topic.topic_id);
                let partition_length = 1;
                for _ in 0..partition_length {
                    buffer.put_i8(partition_length + 1);
                    buffer.put_i32(0); // Index
                    buffer.put_i16(100);
                    buffer.put_i64(0);
                    buffer.put_i64(0);
                    buffer.put_i64(0);
                    buffer.put_i8(0);
                    buffer.put_i32(0);
                    buffer.put_i8(1);
                    buffer.put_i8(0); // TAG_BUFFER partitions
                }
                buffer.put_i8(0); // TAG_BUFFER topic
            }
            buffer.put_i8(0); // no tagged fields, null marker
        }
    }
}

mod tests {
    use super::*;
    use crate::server::{model::Topic, ErrorCode};
    use api_versions::ApiVersionV4Response;

    #[test]
    fn test_api_version_to_wire_format() {
        let mut buffer = vec![];
        let response: Result<ApiVersionV4Response, ErrorCode> = Ok(ApiVersionV4Response {
            api_key_versions: vec![super::ApiKeyVariant::Fetch, super::ApiKeyVariant::Versions],
            throttle_time_in_ms: 0,
        });
        response.to_wire_format(&mut buffer);

        assert_eq!(
            buffer,
            vec![0, 0, 3, 0, 1, 0, 0, 0, 16, 0, 0, 18, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_fetch_response_to_wire_format() {
        let mut buffer = vec![];
        let response = fetch::FetchV16Response::process_request(&FetchRequest {
            session_id: 0,
            topics: vec![Topic { id: 17 }],
        });
        response.to_wire_format(&mut buffer);

        assert_eq!(
            buffer,
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                17, 2, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0
            ]
        );
    }
}
