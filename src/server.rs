use std::io::{self, Read, Write};
use std::net::TcpListener;

use bytes::{Buf, BufMut};

fn handle_connection<T: Read + Write>(mut stream: T) -> io::Result<usize> {
    let mut len = [0; 4];
    stream.read_exact(&mut len)?;
    let len = i32::from_be_bytes(len) as usize;

    let mut request = vec![0; len];
    stream.read_exact(&mut request)?;

    let mut request = request.as_slice();

    let _request_api_key = request.get_i16();
    let request_api_version = request.get_i16();
    let correlation_id: i32 = request.get_i32();

    let mut response: Vec<u8> = Vec::with_capacity(8);
    response.put_i32(0);
    response.put_i32(correlation_id);

    if !(0 <= request_api_version && request_api_version <= 4) {
        response.put_i16(ErrorCode::UnsupportedVersion as i16);
    }

    stream.write(&response)
}

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
        let mut request: Vec<u8> = vec![];
        request.put_i32(35);
        request.put_i16(18);
        request.put_i16(4);
        request.put_i32(311908132);
        request.put_bytes(0, 27);
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(request);

        let result = handle_connection(&mut cursor);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 8);

        assert_eq!(
            &cursor.get_ref()[39..47],
            &vec![0, 0, 0, 0, 18, 151, 87, 36][..]
        );
    }

    #[test]
    fn test_handle_connection_should_fail_with_body_35_if_the_api_version_is_incorrect() {
        let mut request: Vec<u8> = vec![];
        request.put_i32(35);
        request.put_i16(18);
        request.put_i16(-1);
        request.put_i32(311908132);
        request.put_bytes(0, 27);
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(request);

        let result = handle_connection(&mut cursor);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        assert_eq!(
            &cursor.get_ref()[35..49],
            &vec![0, 0, 0, 0, 0, 0, 0, 0, 18, 151, 87, 36, 0, 35][..]
        );
    }
}
