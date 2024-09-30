use super::model;
use super::model::WireSerialization;

#[derive(Debug, PartialEq)]
pub enum Response {
    ApiVersions(ApiVersions),
    Fetch(Fetch),
}

impl WireSerialization for Response {
    fn to_wire_format(&self, buffer: &mut Vec<u8>) {
        match self {
            Response::ApiVersions(api_versions_response) => {
                api_versions_response.to_wire_format(buffer)
            }
            Response::Fetch(fetch_response) => fetch_response.to_wire_format(buffer),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Fetch {
    pub throttle_time_in_ms: i32,
    pub session_id: i32,
    pub topics: Vec<fetch::FetchTopicResponse>,
}

#[derive(Debug, PartialEq)]
pub struct ApiVersions {
    pub api_key_versions: Vec<model::ApiKeyVariant>,
    pub throttle_time_in_ms: i32,
}

pub mod api_versions {
    use bytes::BufMut;

    use crate::server::ErrorCode;

    impl super::WireSerialization for super::ApiVersions {
        // https://kafka.apache.org/protocol#The_Messages_ApiVersions
        fn to_wire_format(&self, buffer: &mut Vec<u8>) -> () {
            buffer.put_i16(ErrorCode::Ok as i16);

            let number_of_tagged_fields = self.api_key_versions.len() + 1; // verint offset by 1 to reserve 0 for null values
            buffer.put_i8(number_of_tagged_fields as i8); // might be buggy for number > 7 as this is encoded as variable length encoding TODO: fix

            for api_key_version in &self.api_key_versions {
                api_key_version.to_wire_format(buffer);
            }
            buffer.put_i32(self.throttle_time_in_ms); // throttle time in ms
            buffer.put_i8(0); // no tagged fields, null marker
        }
    }
}

pub mod fetch {
    use bytes::BufMut;

    #[derive(Debug, PartialEq)]
    pub struct FetchTopicResponse {
        pub topic_id: u128,
    }

    impl super::WireSerialization for super::Fetch {
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
    use fetch::FetchTopicResponse;

    use super::*;

    #[test]
    fn test_api_version_to_wire_format() {
        let mut buffer = vec![];
        let api_versions_response = ApiVersions {
            api_key_versions: vec![
                super::model::ApiKeyVariant::Fetch,
                super::model::ApiKeyVariant::Versions,
            ],
            throttle_time_in_ms: 0,
        };

        api_versions_response.to_wire_format(&mut buffer);

        assert_eq!(
            buffer,
            vec![0, 0, 3, 0, 1, 0, 0, 0, 16, 0, 0, 18, 0, 1, 0, 4, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_fetch_response_to_wire_format() {
        let mut buffer = vec![];
        let response = super::Response::Fetch(Fetch {
            throttle_time_in_ms: 0,
            session_id: 0,
            topics: vec![FetchTopicResponse { topic_id: 17 }],
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
