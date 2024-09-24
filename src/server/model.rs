#[derive(Debug)]
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
