use model::ApiKey;
use model::ApiKeyVariant;
use model::RequestHeader;
use model::WireSerialization;
use responses::ApiVersionV4Response;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use std::error::Error;

use bytes::{Buf, BufMut};

mod model;
mod responses;

pub async fn start_server(address: &str) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(address).await.unwrap();
    println!("Server listening on {}", address);

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            loop {
                if let Err(e) = process(&mut stream).await {
                    eprintln!("Error processing connection: {}", e)
                }
            }
        });
    }
}

async fn process(stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let request = parse_request(stream).await?;
    let response: Vec<u8> = handle_request(&request);
    stream.write(&response[..]).await?;
    Ok(())
}

async fn parse_request<R>(stream: &mut R) -> Result<RequestHeader, Box<dyn Error>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0; 4];
    stream.read_exact(&mut len).await?;
    let len = i32::from_be_bytes(len) as usize;

    let mut request = vec![0; len];
    stream.read_exact(&mut request).await?;

    let mut request = request.as_slice();

    let request_header = model::RequestHeader {
        request_api_key: request.get_i16(),
        request_api_version: request.get_i16(),
        correlation_id: request.get_i32(),
    };
    Ok(request_header)
}

fn handle_request(request: &RequestHeader) -> Vec<u8> {
    let mut response: Vec<u8> = vec![];

    let error_code = if !request.is_request_api_version_valid() {
        ErrorCode::UnsupportedVersion
    } else {
        ErrorCode::Ok
    };

    if error_code != ErrorCode::Ok {
        response.put_i32(4 + 2); // correlation_id and error_code
        response.put_i32(request.correlation_id);
        response.put_i16(error_code as i16);
        return response;
    }

    let data = if request.request_api_key == ApiKey::Versions as i16 {
        let mut buffer: Vec<u8> = vec![];
        let response =
            ApiVersionV4Response::new(vec![ApiKeyVariant::Fetch, ApiKeyVariant::Versions]);
        response.to_wire_format(&mut buffer);
        buffer
    } else {
        vec![]
    };

    let length = 4 + 2 + data.len(); // correlation id + error code
    response.put_i32(length as i32);
    response.put_i32(request.correlation_id);
    response.put_i16(error_code as i16);
    response.put(&data[..]);
    response
}

#[repr(i16)]
#[derive(PartialEq)]
enum ErrorCode {
    Ok = 0,
    UnsupportedVersion = 35,
}

// //TODO: refacto how ErrorCode is injected (should it even be injected this way or hardcoded?)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_request() {
        let result = handle_request(&RequestHeader {
            request_api_key: 0,
            request_api_version: 0,
            correlation_id: 311908132,
        });

        assert_eq!(
            result,
            vec![0, 0, 0, 6, 18, 151, 87, 36, 0, ErrorCode::Ok as u8]
        );
    }

    #[test]
    fn test_handle_connection_should_fail_with_body_35_if_the_api_version_is_incorrect() {
        let result = handle_request(&RequestHeader {
            request_api_key: ApiKey::Versions as i16,
            request_api_version: -1,
            correlation_id: 311908132,
        });

        assert_eq!(
            result,
            vec![
                0,
                0,
                0,
                6,
                18,
                151,
                87,
                36,
                0,
                ErrorCode::UnsupportedVersion as u8
            ]
        );
    }

    #[tokio::test]
    async fn test_parse_request() {
        let (mut client, mut server) = tokio::io::duplex(512);

        let request_body_length = 8;
        let request_api_key: i16 = 18;
        let request_api_version: i16 = 2;
        let correlation_id: i32 = 42;

        let mut request_data = vec![];
        request_data.put_i32(request_body_length as i32);
        request_data.put_i16(request_api_key);
        request_data.put_i16(request_api_version);
        request_data.put_i32(correlation_id);

        client.write_all(&request_data).await.unwrap();
        client.shutdown().await.unwrap();

        // Parse the request on the "server" side
        let request_header = parse_request(&mut server).await.unwrap();

        assert_eq!(
            request_header,
            RequestHeader {
                request_api_key: request_api_key,
                request_api_version: request_api_version,
                correlation_id: correlation_id,
            }
        );
    }
}
