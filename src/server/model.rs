use bytes::BufMut;
use std::error::Error;

pub trait WireSerialization {
    fn to_wire_format(&self, buffer: &mut Vec<u8>) -> ();
}

#[repr(i16)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ApiKey {
    Fetch = 1,
    Versions = 18,
}

impl ApiKey {
    pub fn parse(value: i16) -> Result<ApiKey, Box<dyn Error>> {
        match value {
            1 => Ok(ApiKey::Fetch),
            18 => Ok(ApiKey::Versions),
            _ => Err(Box::from("api key not recognized")),
        }
    }
}

// TODO: should not be public
pub struct ApiKeyVersions {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiKeyVersions {
    pub fn is_version_valid(&self, version: i16) -> bool {
        self.min_version <= version && version <= self.max_version
    }
}

pub enum ApiKeyVariant {
    Fetch,
    Versions,
}

impl ApiKeyVariant {
    pub fn versions(&self) -> ApiKeyVersions {
        match self {
            ApiKeyVariant::Versions => ApiKeyVersions {
                api_key: ApiKey::Versions,
                min_version: 1,
                max_version: 4,
            },
            ApiKeyVariant::Fetch => ApiKeyVersions {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 16,
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
