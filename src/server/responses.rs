use super::model::ApiKeyVariant;
use super::model::WireSerialization;
use bytes::BufMut;

pub struct ApiVersionV4Response {
    api_key_versions: Vec<ApiKeyVariant>,
    throttle_time_in_ms: i32,
}

impl ApiVersionV4Response {
    pub fn new(api_key_versions: Vec<ApiKeyVariant>) -> ApiVersionV4Response {
        ApiVersionV4Response {
            api_key_versions: api_key_versions,
            throttle_time_in_ms: 0,
        }
    }
}

impl WireSerialization for ApiVersionV4Response {
    // https://kafka.apache.org/protocol#The_Messages_ApiVersions
    fn to_wire_format(&self, buffer: &mut Vec<u8>) -> () {
        let number_of_tagged_fields = self.api_key_versions.len() + 1; // verint offset by 1 to reserve 0 for null values
        buffer.put_i8(number_of_tagged_fields as i8); // might be buggy for number > 7 as this is encoded as variable length encoding TODO: fix

        for api_key_version in &self.api_key_versions {
            api_key_version.to_wire_format(buffer);
        }
        buffer.put_i32(self.throttle_time_in_ms); // throttle time in ms
        buffer.put_i8(0); // no tagged fields, null marker
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_api_version_to_wire_format() {
        let mut buffer = vec![];
        let response =
            ApiVersionV4Response::new(vec![ApiKeyVariant::Fetch, ApiKeyVariant::Versions]);
        response.to_wire_format(&mut buffer);

        assert_eq!(
            buffer,
            vec![3, 0, 1, 0, 0, 0, 16, 0, 0, 18, 0, 1, 0, 18, 0, 0, 0, 0, 0, 0]
        );
    }
}
