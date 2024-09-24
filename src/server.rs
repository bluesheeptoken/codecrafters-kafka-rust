use std::io::{self, Read, Write};
use std::net::TcpListener;

use bytes::{Buf, BufMut};

mod model;

pub fn start_server(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    println!("Server listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                println!("Accepted new connection");
                handle_connection(s)?;
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_connection<T: Read + Write>(mut stream: T) -> io::Result<usize> {
    let mut len = [0; 4];
    stream.read_exact(&mut len)?;
    let len = i32::from_be_bytes(len) as usize;

    let mut request = vec![0; len];
    stream.read_exact(&mut request)?;

    let mut request = request.as_slice();

    let request_header = model::RequestHeader {
        request_api_key: request.get_i16(),
        request_api_version: request.get_i16(),
        correlation_id: request.get_i32(),
    };

    let mut response: Vec<u8> = vec![];
    let mut data: Vec<u8> = vec![];
    if request_header.request_api_key == ApiKey::Versions as i16 {
        data = api_version_response_data();
    }

    let length = 4 + 2 + data.len(); // correlation id + error code
    response.put_i32(length as i32);
    response.put_i32(request_header.correlation_id);

    if !request_header.is_request_api_version_valid() {
        response.put_i16(ErrorCode::UnsupportedVersion as i16);
    } else {
        response.put_i16(ErrorCode::Ok as i16);
    }

    response.put(&data[..]);

    stream.write(&response)
}

// https://kafka.apache.org/protocol#The_Messages_ApiVersions
fn api_version_response_data() -> Vec<u8> {
    let mut data: Vec<u8> = vec![];
    let number_of_tagged_fields = 2; // verint offset by 1 to reserve 0 for null values
    data.put_i8(number_of_tagged_fields); // might be buggy for number > 7 as this is encoded as variable length encoding
    data.put_i16(ApiKey::Versions as i16);
    data.put_i16(0); // min version, todo represent that in a struct
    data.put_i16(4); // max version, todo represent that in a struct
    data.put_i8(0); // no tagged fields, null marker
    data.put_i32(0); // throttle time in ms
    data.put_i8(0); // no tagged fields, null marker
    data
}

#[repr(i16)]
enum ErrorCode {
    Ok = 0,
    UnsupportedVersion = 35,
}

#[repr(i16)]
enum ApiKey {
    Versions = 18,
}

//TODO: refacto how ErrorCode is injected (should it even be injected this way or hardcoded?)
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_handle_connection() {
        let mut request = RequestBuilder::new().with_correlation_id(311908132).build();

        let result = handle_connection(&mut request);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        assert_eq!(
            &request.get_ref()[12..22],
            &vec![0, 0, 0, 6, 18, 151, 87, 36, 0, ErrorCode::Ok as u8][..]
        );
    }

    #[test]
    fn test_handle_connection_with_api_versions_request() {
        let mut request = RequestBuilder::new()
            .with_correlation_id(311908132)
            .with_request_api_key(ApiKey::Versions as i16)
            .build();

        let result = handle_connection(&mut request);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 23);

        assert_eq!(
            &request.get_ref()[12..35],
            &vec![
                0,
                0,
                0,
                19,
                18,
                151,
                87,
                36,
                0,
                ErrorCode::Ok as u8,
                2,
                0,
                18,
                0,
                0,
                0,
                4,
                0,
                0,
                0,
                0,
                0,
                0,
            ][..]
        );
    }

    #[test]
    fn test_handle_connection_should_fail_with_body_35_if_the_api_version_is_incorrect() {
        let mut request = RequestBuilder::new()
            .with_correlation_id(311908132)
            .with_request_api_version(-1)
            .build();

        let result = handle_connection(&mut request);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        assert_eq!(
            &request.get_ref()[12..22],
            &vec![
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
            ][..]
        );
    }

    struct RequestBuilder {
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
    }
    impl RequestBuilder {
        fn new() -> RequestBuilder {
            RequestBuilder {
                request_api_key: 0,
                request_api_version: 0,
                correlation_id: 0,
            }
        }

        fn with_request_api_key(&mut self, request_api_key: i16) -> &mut RequestBuilder {
            self.request_api_key = request_api_key;
            self
        }

        fn with_request_api_version(&mut self, request_api_version: i16) -> &mut RequestBuilder {
            self.request_api_version = request_api_version;
            self
        }
        fn with_correlation_id(&mut self, correlation_id: i32) -> &mut RequestBuilder {
            self.correlation_id = correlation_id;
            self
        }

        fn build(&self) -> Cursor<Vec<u8>> {
            let mut request: Vec<u8> = vec![];
            request.put_i32(8);
            request.put_i16(self.request_api_key);
            request.put_i16(self.request_api_version);
            request.put_i32(self.correlation_id);
            Cursor::new(request)
        }
    }
}
