use super::model::ApiKey;
use super::model::ApiKeyVariant;
use bytes::Buf;
use std::error::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct Request {
    pub header: RequestHeader,
    pub body: Box<dyn RequestBody>,
}
#[derive(Debug, PartialEq)]
pub struct RequestHeader {
    pub request_api_key: ApiKey,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl Request {
    // TODO: Weird conversion here
    pub fn is_request_api_version_valid(&self, version: i16) -> bool {
        match self.header.request_api_key {
            ApiKey::Versions => (ApiKeyVariant::Versions)
                .versions()
                .is_version_valid(version),
            ApiKey::Fetch => (ApiKeyVariant::Fetch).versions().is_version_valid(version),
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

        let mut request = request.as_slice();

        let request_header = RequestHeader {
            request_api_key: ApiKey::parse(request.get_i16())?,
            request_api_version: request.get_i16(),
            correlation_id: request.get_i32(),
        };

        let body: Box<dyn RequestBody> = match request_header.request_api_key {
            ApiKey::Versions => Box::new(ApiVersionsRequest {}),
            ApiKey::Fetch => Box::new(FetchRequest {}),
        };

        Ok(Request {
            header: request_header,
            body: body,
        })
    }
}

pub trait RequestBody: Send {}

#[derive(Debug, PartialEq)]
pub struct ApiVersionsRequest {}

impl RequestBody for ApiVersionsRequest {}

#[derive(Debug, PartialEq)]
pub struct FetchRequest {}

impl RequestBody for FetchRequest {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use tokio::io::AsyncWriteExt;
    #[tokio::test]
    async fn test_parse_request_api_versions_request() {
        let (mut client, mut server) = tokio::io::duplex(512);

        let request_body_length = 8;
        let request_api_key: ApiKey = ApiKey::Versions;
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
}
