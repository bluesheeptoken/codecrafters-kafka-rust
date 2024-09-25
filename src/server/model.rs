use bytes::BufMut;

// TODO: move that to a specific file once we implement request parsing
#[derive(Debug, PartialEq)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl RequestHeader {
    pub fn is_request_api_version_valid(&self) -> bool {
        self.request_api_version >= 0 && self.request_api_version <= 4
    }
}

pub trait WireSerialization {
    fn to_wire_format(&self, buffer: &mut Vec<u8>) -> ();
}

#[repr(i16)]
#[derive(Copy, Clone)]
pub enum ApiKey {
    Fetch = 1,
    Versions = 18,
}
struct ApiKeyVersions {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

pub enum ApiKeyVariant {
    Fetch,
    Versions,
}

impl ApiKeyVariant {
    fn versions(&self) -> ApiKeyVersions {
        match self {
            ApiKeyVariant::Fetch => ApiKeyVersions {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 16,
            },
            ApiKeyVariant::Versions => ApiKeyVersions {
                api_key: ApiKey::Versions,
                min_version: 1,
                max_version: 18,
            },
        }
    }
}

impl WireSerialization for ApiKeyVariant {
    fn to_wire_format(&self, buffer: &mut Vec<u8>) -> () {
        let version_info = self.versions();
        buffer.put_i16(version_info.api_key as i16);
        buffer.put_i16(version_info.min_version);
        buffer.put_i16(version_info.max_version);
        buffer.put_i8(0); // no tagged fields, null marker
    }
}
