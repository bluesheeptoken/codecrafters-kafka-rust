use std::io::{self, Write};
use std::net::TcpListener;

fn write_api_versions<T: Write>(mut stream: T) -> io::Result<usize> {
    let buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 7];
    stream.write(&buf)
}

pub fn start_server(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    println!("Server listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                println!("Accepted new connection");
                let _size = write_api_versions(s)?;
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_handle_connection() {
        let mut cursor: Cursor<Vec<u8>> = Cursor::new(vec![]);

        let result = write_api_versions(&mut cursor);

        assert!(result.is_ok());
        assert_eq!(cursor.get_ref().as_slice(), &[0, 0, 0, 0, 0, 0, 0, 7]);
    }
}
