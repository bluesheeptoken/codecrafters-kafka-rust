use model::ApiKey;
use model::ApiKeyVariant;
use model::WireSerialization;
use requests::Request;
use responses::ApiVersionV4Response;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use std::error::Error;

use bytes::BufMut;

mod model;
mod requests;
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
    let request = Request::parse_request(stream).await?;
    let response: Vec<u8> = handle_request(&request);
    stream.write(&response[..]).await?;
    Ok(())
}

fn handle_request(request: &Request) -> Vec<u8> {
    let mut response: Vec<u8> = vec![];

    let error_code = if !request.is_request_api_version_valid() {
        ErrorCode::UnsupportedVersion
    } else {
        ErrorCode::Ok
    };

    if error_code != ErrorCode::Ok {
        response.put_i32(4 + 2); // correlation_id and error_code
        response.put_i32(request.header.correlation_id);
        response.put_i16(error_code as i16);
        return response;
    }

    let data = match request.header.request_api_key {
        ApiKey::Versions => {
            let mut buffer: Vec<u8> = vec![];
            let response =
                ApiVersionV4Response::new(vec![ApiKeyVariant::Fetch, ApiKeyVariant::Versions]);
            response.to_wire_format(&mut buffer);
            buffer
        }
        _ => vec![],
    };

    let length = 4 + 2 + data.len(); // correlation id + error code
    response.put_i32(length as i32);
    response.put_i32(request.header.correlation_id);
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
    use requests::ApiVersionsRequest;

    use super::*;

    use super::requests::RequestHeader;

    #[test]
    fn test_handle_versions_request() {
        let result = handle_request(&Request {
            header: RequestHeader {
                request_api_key: ApiKey::Versions,
                request_api_version: 0,
                correlation_id: 311908132,
            },
            body: Box::new(ApiVersionsRequest {}),
        });

        assert_eq!(
            result,
            vec![
                0, 0, 0, 26, 18, 151, 87, 36, 0, 0, 3, 0, 1, 0, 0, 0, 16, 0, 0, 18, 0, 1, 0, 18, 0,
                0, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn test_handle_connection_should_fail_with_body_35_if_the_api_version_is_incorrect() {
        let result = handle_request(&Request {
            header: RequestHeader {
                request_api_key: ApiKey::Versions,
                request_api_version: -1,
                correlation_id: 311908132,
            },
            body: Box::new(ApiVersionsRequest {}),
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
    async fn test_parse_reques_api_versions_request() {
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

        // Parse the request on the "server" side
        let request = Request::parse_request(&mut server).await.unwrap();
        let expected_header = RequestHeader {
            request_api_key: request_api_key,
            request_api_version: request_api_version,
            correlation_id: correlation_id,
        };

        assert_eq!(request.header, expected_header);
        // TODO: check body???
    }
}
