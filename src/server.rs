use model::WireSerialization;
use requests::Request;
use requests::RequestBody;
use responses::api_versions::ApiVersionV4Response;
use responses::fetch;
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

    let error_code = if !request.is_request_api_version_valid(request.header.request_api_version) {
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

    //TODO: some copy paste that could be handled generically
    let data = match &request.body {
        RequestBody::ApiVersions(body) => {
            let mut buffer: Vec<u8> = vec![];
            let response = ApiVersionV4Response::process_request(body, error_code);
            response.to_wire_format(&mut buffer);
            buffer
        }
        RequestBody::Fetch(body) => {
            let mut buffer: Vec<u8> = vec![];
            let response = fetch::FetchV16Response::process_request(&body);
            response.to_wire_format(&mut buffer);
            buffer
        }
    };

    let length = 4 + data.len(); // correlation id + error code
    response.put_i32(length as i32);
    response.put_i32(request.header.correlation_id);
    response.put(&data[..]);
    response
}

#[repr(i16)]
#[derive(Copy, Clone, PartialEq)]
enum ErrorCode {
    Ok = 0,
    UnsupportedVersion = 35,
}

// //TODO: refacto how ErrorCode is injected (should it even be injected this way or hardcoded?)
#[cfg(test)]
mod tests {
    use requests::ApiVersionsRequest;

    use super::*;

    use super::model::ApiKey;
    use super::requests::RequestHeader;

    #[test]
    fn test_handle_versions_request() {
        let result = handle_request(&Request {
            header: RequestHeader {
                request_api_key: ApiKey::Versions,
                request_api_version: 4,
                correlation_id: 311908132,
            },
            body: requests::RequestBody::ApiVersions(ApiVersionsRequest {}),
        });

        assert_eq!(
            result,
            vec![
                0, 0, 0, 26, 18, 151, 87, 36, 0, 0, 3, 0, 1, 0, 0, 0, 16, 0, 0, 18, 0, 1, 0, 4, 0,
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
            body: requests::RequestBody::ApiVersions(ApiVersionsRequest {}),
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
}
