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
        _request_api_key: request.get_i16(),
        request_api_version: request.get_i16(),
        correlation_id: request.get_i32(),
    };

    let mut response: Vec<u8> = Vec::with_capacity(8);
    response.put_i32(0);
    response.put_i32(request_header.correlation_id);

    if !request_header.is_request_api_version_valid() {
        response.put_i16(ErrorCode::UnsupportedVersion as i16);
    }

    stream.write(&response)
}

#[repr(i16)]
enum ErrorCode {
    UnsupportedVersion = 35,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_handle_connection() {
        let mut request = RequestBuilder::new().with_correlation_id(311908132).build();

        let result = handle_connection(&mut request);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 8);

        assert_eq!(
            &request.get_ref()[12..20],
            &vec![0, 0, 0, 0, 18, 151, 87, 36][..]
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
            &vec![0, 0, 0, 0, 18, 151, 87, 36, 0, 35][..]
        );
    }

    struct RequestBuilder {
        message_length_after_header: usize,
        request_api_key: i16,
        request_api_version: i16,
        correlation_id: i32,
    }
    impl RequestBuilder {
        fn new() -> RequestBuilder {
            RequestBuilder {
                message_length_after_header: 0,
                request_api_key: 0,
                request_api_version: 0,
                correlation_id: 0,
            }
        }

        fn with_message_length_after_header(
            &mut self,
            message_length_after_header: usize,
        ) -> &mut RequestBuilder {
            self.message_length_after_header = message_length_after_header;
            self
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
            request.put_i32(self.message_length_after_header as i32 + 8);
            request.put_i16(self.request_api_key);
            request.put_i16(self.request_api_version);
            request.put_i32(self.correlation_id);
            request.put_bytes(0, self.message_length_after_header);
            Cursor::new(request)
        }
    }
}
